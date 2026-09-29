"""
Matchmaker для Cubism D.
Авто-подбор: хост создаёт комнату, матчмейкер ищет игроков.
Деплой на Render.com.
"""

import asyncio
import json
import logging
import os
import signal
import uuid
from dataclasses import dataclass, field
from typing import Optional

import websockets

logging.basicConfig(
    level=logging.INFO,
    format="[MM] %(asctime)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MODES = {
    "classic":       {"min_players": 2, "max_players": 4, "timeout_sec": 30},
    "battle_royale": {"min_players": 4, "max_players": 20, "timeout_sec": 60},
}


@dataclass
class Room:
    match_id: str
    mode: str
    map_name: str
    host_nickname: str
    host_ws: object
    host_oid: str
    max_players: int
    players: list = field(default_factory=list)  # список (ws, nickname)
    match_started: bool = False


class Matchmaker:
    def __init__(self):
        self.rooms: dict[str, Room] = {}
        self.player_to_room: dict[int, str] = {}  # id(ws) -> match_id
        self.timers: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    async def handle(self, ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                msg_type = msg.get("type")

                if msg_type == "create_room":
                    await self.create_room(ws, msg)

                elif msg_type == "find_match":
                    await self.find_match(ws, msg)

                elif msg_type == "leave":
                    await self.remove_player(ws)

                elif msg_type == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.remove_player(ws)

    async def create_room(self, ws, msg):
        async with self.lock:
            match_id = str(uuid.uuid4())[:6].upper()
            mode = msg.get("mode", "classic")
            if mode not in MODES:
                mode = "classic"

            room = Room(
                match_id=match_id,
                mode=mode,
                map_name=msg.get("map", "Island"),
                host_nickname=msg.get("host_nickname", "Host"),
                host_ws=ws,
                host_oid=msg.get("noray_oid", ""),
                max_players=min(int(msg.get("max_players", MODES[mode]["max_players"])),
                                MODES[mode]["max_players"]),
            )
            room.players.append((ws, room.host_nickname))
            self.rooms[match_id] = room
            self.player_to_room[id(ws)] = match_id

            log.info(f"Комната {match_id} ({mode}, {room.map_name}) создана хостом {room.host_nickname}. OID={room.host_oid}")

            await ws.send(json.dumps({
                "type": "room_created",
                "match_id": match_id,
            }))

            # Таймер на добор игроков
            self.timers[match_id] = asyncio.create_task(self._room_timer(match_id))

    async def find_match(self, ws, msg):
        """Игрок хочет играть. Ищем комнату или создаём новую."""
        async with self.lock:
            mode = msg.get("mode", "classic")
            map_name = msg.get("map", "Island")
            nickname = msg.get("nickname", "Player")

            # Ищем комнату с местом
            room = None
            for r in self.rooms.values():
                if (r.mode == mode
                        and r.map_name == map_name
                        and not r.match_started
                        and len(r.players) < r.max_players):
                    room = r
                    break

            if room is None:
                # Нет комнаты — создаём, игрок становится хостом
                # Но у него ещё нет noray_oid — он получит его от клиента
                # В этом случае мы ждём, пока клиент сам не создаст noray-комнату
                # и не отправит register_room. Пока — отвечаем "нужно создать".
                await ws.send(json.dumps({
                    "type": "no_room",
                    "mode": mode,
                    "map": map_name,
                    "message": "Нет комнат. Создай комнату через Create Room.",
                }))
                return

            # Присоединяем
            room.players.append((ws, nickname))
            self.player_to_room[id(ws)] = room.match_id
            log.info(f"{nickname} присоединился к {room.match_id}. Игроков: {len(room.players)}/{room.max_players}")

            # Отправляем OID хоста
            await ws.send(json.dumps({
                "type": "match_ready",
                "match_id": room.match_id,
                "noray_oid": room.host_oid,
                "mode": room.mode,
                "map": room.map_name,
            }))

            # Проверяем, набралось ли минимум
            await self._check_ready(room)

    async def _check_ready(self, room: Room):
        if room.match_started:
            return
        if len(room.players) < MODES[room.mode]["min_players"]:
            return

        # Все игроки набраны — команда старт
        room.match_started = True
        log.info(f"Матч {room.match_id} стартует. Игроков: {len(room.players)}")

        for (pws, _) in room.players:
            try:
                await pws.send(json.dumps({
                    "type": "start_match",
                    "match_id": room.match_id,
                }))
            except Exception as e:
                log.error(f"Не отправить start_match: {e}")

    async def _room_timer(self, match_id: str):
        await asyncio.sleep(MODES["classic"]["timeout_sec"])
        async with self.lock:
            room = self.rooms.get(match_id)
            if room and not room.match_started:
                if len(room.players) >= MODES[room.mode]["min_players"]:
                    await self._check_ready(room)
                else:
                    log.info(f"Комната {match_id} закрыта по таймауту (мало игроков)")

    async def remove_player(self, ws):
        async with self.lock:
            match_id = self.player_to_room.pop(id(ws), None)
            if not match_id:
                return
            room = self.rooms.get(match_id)
            if not room:
                return

            # Удаляем игрока из комнаты
            room.players = [(p, n) for (p, n) in room.players if p is not ws]

            # Если хост ушёл — удаляем комнату
            if room.host_ws is ws:
                log.info(f"Хост ушёл, комната {match_id} удалена")
                for (pws, _) in room.players:
                    try:
                        await pws.send(json.dumps({"type": "room_closed"}))
                    except Exception:
                        pass
                del self.rooms[match_id]
                if match_id in self.timers:
                    self.timers[match_id].cancel()
                    del self.timers[match_id]
            else:
                log.info(f"Игрок вышел из {match_id}. Осталось: {len(room.players)}")


async def health_check(connection, request):
    if request.path == "/healthz":
        return connection.respond(200, "OK\n")
    if "Upgrade" in request.headers and request.headers["Upgrade"].lower() == "websocket":
        return None
    return connection.respond(404, "Not Found\n")


async def main():
    mm = Matchmaker()
    port = int(os.environ.get("PORT", 8765))
    log.info(f"Matchmaker запущен на 0.0.0.0:{port}")

    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _on_signal():
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _on_signal)
        except NotImplementedError:
            pass

    async with websockets.serve(mm.handle, "0.0.0.0", port, process_request=health_check):
        await stop


if __name__ == "__main__":
    asyncio.run(main())
