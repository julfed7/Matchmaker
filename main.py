"""
Matchmaker для Cubism D.
- classic: 2-4 игрока.
- battle_royale: 10-20 игроков.
- Комната ОТКРЫТА, пока не набран max_players И хост не начал матч.
- Как только match_started = True — комната больше не находится в find_match.
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
    "classic": {
        "min_players": 2,
        "max_players": 4,
        "timeout_sec": 120,
    },
    "battle_royale": {
        "min_players": 10,
        "max_players": 20,
        "timeout_sec": 180,
    },
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
    players: list = field(default_factory=list)  # [(ws, nickname), ...]
    match_started: bool = False


class Matchmaker:
    def __init__(self):
        self.rooms: dict[str, Room] = {}
        self.oid_to_match: dict[str, str] = {}
        self.player_to_room: dict[int, str] = {}
        self.timers: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    async def handle(self, ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                t = msg.get("type")
                log.info(f"← {t}")

                if t == "create_room":
                    await self.create_room(ws, msg)
                elif t == "find_match":
                    await self.find_match(ws, msg)
                elif t == "join_by_oid":
                    await self.join_by_oid(ws, msg)
                elif t == "start_match":
                    await self.start_match(ws, msg)
                elif t == "leave":
                    await self.remove_player(ws)
                elif t == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.remove_player(ws)

    # === CREATE ROOM ===

    async def create_room(self, ws, msg):
        async with self.lock:
            mode = msg.get("mode", "classic")
            if mode not in MODES:
                mode = "classic"

            match_id = str(uuid.uuid4())[:6].upper()
            room = Room(
                match_id=match_id,
                mode=mode,
                map_name=msg.get("map", "Island"),
                host_nickname=msg.get("host_nickname", "Host"),
                host_ws=ws,
                host_oid=msg.get("noray_oid", ""),
                max_players=min(
                    int(msg.get("max_players", MODES[mode]["max_players"])),
                    MODES[mode]["max_players"],
                ),
            )
            room.players.append((ws, room.host_nickname))
            self.rooms[match_id] = room
            if room.host_oid:
                self.oid_to_match[room.host_oid] = match_id
            self.player_to_room[id(ws)] = match_id

            log.info(
                f"✓ Комната {match_id} ({mode}/{room.map_name}) создана "
                f"хостом {room.host_nickname}. OID={room.host_oid} "
                f"min={MODES[mode]['min_players']} max={room.max_players}"
            )

            await ws.send(json.dumps({
                "type": "room_created",
                "match_id": match_id,
                "min_players": MODES[mode]["min_players"],
                "max_players": room.max_players,
            }))

            self.timers[match_id] = asyncio.create_task(self._room_timer(match_id))

    # === FIND MATCH ===

    async def find_match(self, ws, msg):
        async with self.lock:
            mode = msg.get("mode", "classic")
            map_name = msg.get("map", "Island")
            nickname = msg.get("nickname", "Player")

            if mode not in MODES:
                mode = "classic"

            log.info(f"Поиск {nickname}: {mode}/{map_name}")

            # КРИТИЧНО: ищем комнаты, где:
            # - режим совпадает
            # - карта совпадает
            # - матч НЕ начался
            # - есть свободные места (players < max_players, НЕ min_players)
            room = None
            for r in self.rooms.values():
                if (r.mode == mode
                        and r.map_name == map_name
                        and not r.match_started
                        and len(r.players) < r.max_players):
                    room = r
                    log.info(f"  Найдена комната {r.match_id}: {len(r.players)}/{r.max_players}")
                    break

            if room is None:
                log.info(f"✗ {nickname}: нет открытых комнат {mode}/{map_name}")
                await ws.send(json.dumps({
                    "type": "no_room",
                    "mode": mode,
                    "map": map_name,
                    "message": "Нет открытых комнат. Создай комнату через Create Room.",
                }))
                return

            # Присоединяем
            room.players.append((ws, nickname))
            self.player_to_room[id(ws)] = room.match_id
            log.info(
                f"✓ {nickname} → {room.match_id}. "
                f"Игроков: {len(room.players)}/{room.max_players}"
            )

            await ws.send(json.dumps({
                "type": "match_ready",
                "match_id": room.match_id,
                "noray_oid": room.host_oid,
                "mode": room.mode,
                "map": room.map_name,
                "players_count": len(room.players),
                "max_players": room.max_players,
                "min_players": MODES[room.mode]["min_players"],
            }))

            # Авто-старт ТОЛЬКО при max_players
            if len(room.players) >= room.max_players:
                log.info(f"Матч {room.match_id}: достигнут максимум {room.max_players}, авто-старт")
                await self._start_room(room, auto=True)

    # === JOIN BY OID ===

    async def join_by_oid(self, ws, msg):
        async with self.lock:
            host_oid = msg.get("noray_oid", "").strip()
            nickname = msg.get("nickname", "Player")

            if not host_oid:
                await ws.send(json.dumps({"type": "error", "message": "Пустой OID"}))
                return

            match_id = self.oid_to_match.get(host_oid)
            if not match_id or match_id not in self.rooms:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Комната с таким OID не найдена",
                }))
                return

            room = self.rooms[match_id]
            if room.match_started:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Матч уже начался",
                }))
                return
            if len(room.players) >= room.max_players:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Комната заполнена",
                }))
                return

            room.players.append((ws, nickname))
            self.player_to_room[id(ws)] = match_id
            log.info(f"✓ {nickname} → {match_id} (по OID). {len(room.players)}/{room.max_players}")

            await ws.send(json.dumps({
                "type": "match_ready",
                "match_id": room.match_id,
                "noray_oid": room.host_oid,
                "mode": room.mode,
                "map": room.map_name,
                "players_count": len(room.players),
                "max_players": room.max_players,
                "min_players": MODES[room.mode]["min_players"],
            }))

    # === START MATCH (от хоста) ===

    async def start_match(self, ws, msg):
        async with self.lock:
            match_id = msg.get("match_id", "")
            room = self.rooms.get(match_id)
            if not room:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Комната не найдена",
                }))
                return
            if room.host_ws is not ws:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": "Только хост может начать матч",
                }))
                return
            if room.match_started:
                return

            # Хост может начать, если набралось min_players
            if len(room.players) < MODES[room.mode]["min_players"]:
                await ws.send(json.dumps({
                    "type": "error",
                    "message": f"Нужно минимум {MODES[room.mode]['min_players']} игроков",
                }))
                return

            await self._start_room(room, auto=False)

    async def _start_room(self, room: Room, auto: bool):
        room.match_started = True
        reason = "авто (макс игроков)" if auto else "хост"
        log.info(
            f"✓ Матч {room.match_id} ({room.mode}) стартует [{reason}]. "
            f"Игроков: {len(room.players)}/{room.max_players}"
        )

        for (pws, _) in room.players:
            try:
                await pws.send(json.dumps({
                    "type": "match_started",
                    "match_id": room.match_id,
                    "auto": auto,
                }))
            except Exception as e:
                log.error(f"Не отправить match_started: {e}")

    async def _room_timer(self, match_id: str):
        room = self.rooms.get(match_id)
        if not room:
            return
        await asyncio.sleep(MODES[room.mode]["timeout_sec"])
        async with self.lock:
            room = self.rooms.get(match_id)
            if not room or room.match_started:
                return

            # Если набралось min_players — стартуем
            if len(room.players) >= MODES[room.mode]["min_players"]:
                log.info(f"Таймаут {match_id}: старт с {len(room.players)} игроками")
                await self._start_room(room, auto=True)
            else:
                # Иначе — закрываем
                log.info(f"✗ Комната {match_id} закрыта по таймауту (мало игроков)")
                for (pws, _) in room.players:
                    try:
                        await pws.send(json.dumps({
                            "type": "room_closed",
                            "message": "Недостаточно игроков",
                        }))
                    except Exception:
                        pass
                if room.host_oid in self.oid_to_match:
                    del self.oid_to_match[room.host_oid]
                del self.rooms[match_id]

    # === REMOVE PLAYER ===

    async def remove_player(self, ws):
        async with self.lock:
            match_id = self.player_to_room.pop(id(ws), None)
            if not match_id:
                return
            room = self.rooms.get(match_id)
            if not room:
                return

            room.players = [(p, n) for (p, n) in room.players if p is not ws]

            if room.host_ws is ws:
                log.info(f"✗ Хост ушёл, комната {match_id} удалена")
                for (pws, _) in room.players:
                    try:
                        await pws.send(json.dumps({"type": "room_closed"}))
                    except Exception:
                        pass
                if room.host_oid in self.oid_to_match:
                    del self.oid_to_match[room.host_oid]
                del self.rooms[match_id]
                if match_id in self.timers:
                    self.timers[match_id].cancel()
                    del self.timers[match_id]
            else:
                log.info(f"Игрок вышел из {match_id}. Осталось: {len(room.players)}/{room.max_players}")


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
