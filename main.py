"""
Matchmaker для Cubism D.
Деплой на Render.com.
Режимы: classic (2-4), battle_royale (4-20).
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

# Конфигурация режимов
MODES = {
    "classic": {
        "min_players": 2,
        "max_players": 4,
        "timeout_sec": 30,
    },
    "battle_royale": {
        "min_players": 4,
        "max_players": 20,
        "timeout_sec": 60,
    },
}


@dataclass
class Player:
    player_id: str
    nickname: str
    mode: str
    ws: object
    match_id: Optional[str] = None


@dataclass
class Match:
    match_id: str
    mode: str
    host_id: str
    players: list = field(default_factory=list)
    host_oid: Optional[str] = None


class Matchmaker:
    def __init__(self):
        self.queues: dict[str, list[Player]] = {m: [] for m in MODES}
        self.players: dict[str, Player] = {}
        self.matches: dict[str, Match] = {}
        self.timers: dict[str, asyncio.Task] = {}
        self.lock = asyncio.Lock()

    async def handle(self, ws):
        """Обработка одного WebSocket-подключения."""
        player_id = None
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    log.warning(f"Плохой JSON: {raw[:100]}")
                    continue

                msg_type = msg.get("type")

                if msg_type == "queue":
                    player_id = msg.get("player_id") or str(uuid.uuid4())[:8]
                    mode = msg.get("mode", "classic")
                    nickname = msg.get("nickname", "Player")
                    await self.add_to_queue(player_id, nickname, mode, ws)
                    await ws.send(json.dumps({
                        "type": "queued",
                        "player_id": player_id,
                        "mode": mode,
                    }))

                elif msg_type == "leave_queue":
                    if player_id:
                        await self.remove_from_queue(player_id)

                elif msg_type == "host_ready":
                    oid = msg.get("noray_oid", "")
                    if player_id:
                        await self.on_host_ready(player_id, oid)

                elif msg_type == "ping":
                    await ws.send(json.dumps({"type": "pong"}))

        except websockets.exceptions.ConnectionClosed:
            log.info(f"Соединение закрыто: {player_id}")
        except Exception as e:
            log.error(f"Ошибка в handle: {e}")
        finally:
            if player_id:
                await self.remove_from_queue(player_id)

    async def add_to_queue(self, player_id: str, nickname: str, mode: str, ws):
        async with self.lock:
            if mode not in MODES:
                log.warning(f"Неизвестный режим {mode}, ставлю classic")
                mode = "classic"

            # Если игрок уже в очереди — удалить старую запись
            if player_id in self.players:
                await self._remove_locked(player_id)

            player = Player(player_id, nickname, mode, ws)
            self.players[player_id] = player
            self.queues[mode].append(player)

            count = len(self.queues[mode])
            log.info(f"{nickname} ({player_id}) → очередь {mode}. Всего: {count}")

            # Таймер на добор
            if mode not in self.timers or self.timers[mode].done():
                self.timers[mode] = asyncio.create_task(self.queue_timer(mode))

            await self._try_start_match(mode)

    async def remove_from_queue(self, player_id: str):
        async with self.lock:
            await self._remove_locked(player_id)

    async def _remove_locked(self, player_id: str):
        if player_id not in self.players:
            return
        player = self.players.pop(player_id)
        queue = self.queues.get(player.mode, [])
        if player in queue:
            queue.remove(player)
            log.info(f"{player.nickname} вышел. Осталось: {len(queue)}")

    async def queue_timer(self, mode: str):
        """Таймаут: если долго никто не идёт — старт с теми, кто есть."""
        await asyncio.sleep(MODES[mode]["timeout_sec"])
        async with self.lock:
            queue = self.queues[mode]
            if len(queue) >= MODES[mode]["min_players"]:
                log.info(f"Таймаут {mode}. Запускаю с {len(queue)} игроками")
                await self._create_match(mode)
            elif queue:
                log.info(f"Таймаут {mode}, но игроков мало: {len(queue)}")

    async def _try_start_match(self, mode: str):
        """Проверяет, набралось ли максимум игроков."""
        queue = self.queues[mode]
        if len(queue) >= MODES[mode]["max_players"]:
            await self._create_match(mode)

    async def _create_match(self, mode: str):
        """Формирует матч, выбирает хоста."""
        queue = self.queues[mode]
        if len(queue) < MODES[mode]["min_players"]:
            return

        count = min(len(queue), MODES[mode]["max_players"])
        players = queue[:count]
        self.queues[mode] = queue[count:]

        match_id = str(uuid.uuid4())[:6].upper()
        host = players[0]

        match = Match(
            match_id=match_id,
            mode=mode,
            host_id=host.player_id,
            players=players,
        )
        self.matches[match_id] = match

        for p in players:
            p.match_id = match_id

        log.info(f"Матч {match_id} ({mode}): {count} игроков. Хост: {host.nickname}")

        # Хосту — команда создать комнату
        try:
            await host.ws.send(json.dumps({
                "type": "you_are_host",
                "match_id": match_id,
                "mode": mode,
                "players": [
                    {"id": p.player_id, "nickname": p.nickname}
                    for p in players
                ],
            }))
        except Exception as e:
            log.error(f"Не отправить хосту: {e}")

        # Остальным — ждать OID хоста
        for p in players[1:]:
            try:
                await p.ws.send(json.dumps({
                    "type": "waiting_for_host",
                    "match_id": match_id,
                    "mode": mode,
                    "host_nickname": host.nickname,
                }))
            except Exception as e:
                log.error(f"Не отправить клиенту: {e}")

    async def on_host_ready(self, host_id: str, noray_oid: str):
        """Хост сообщил свой OID — рассылаем всем в его матче."""
        if host_id not in self.players:
            log.warning(f"host_ready от неизвестного {host_id}")
            return

        player = self.players[host_id]
        match_id = player.match_id
        if not match_id or match_id not in self.matches:
            log.warning(f"У {host_id} нет активного матча")
            return

        match = self.matches[match_id]
        match.host_oid = noray_oid
        log.info(f"Матч {match_id}: хост {host_id} готов. OID={noray_oid}")

        # Рассылаем всем клиентам матча
        for p in match.players:
            if p.player_id == host_id:
                continue
            try:
                await p.ws.send(json.dumps({
                    "type": "match_ready",
                    "match_id": match_id,
                    "host_oid": noray_oid,
                    "mode": match.mode,
                }))
            except Exception as e:
                log.error(f"Не отправить OID клиенту {p.player_id}: {e}")


async def health_check(connection, request):
    """HTTP health check для Render."""
    if request.path == "/healthz":
        return connection.respond(200, "OK\n")
    return connection.respond(404, "Not Found\n")


async def main():
    matchmaker = Matchmaker()

    port = int(os.environ.get("PORT", 8765))
    host = "0.0.0.0"

    log.info(f"Matchmaker запущен на ws://{host}:{port}")
    log.info(f"Режимы: classic (2-4), battle_royale (4-20)")

    # Обработка SIGTERM (для корректного деплоя на Render)
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

    async with websockets.serve(
        matchmaker.handle,
        host,
        port,
        process_request=health_check,
    ):
        await stop

    log.info("Matchmaker остановлен")


if __name__ == "__main__":
    asyncio.run(main())
