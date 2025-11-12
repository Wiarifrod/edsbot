import asyncio
import sys
from collections.abc import Awaitable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import aiosqlite
import pytest
from telegram.constants import ParseMode

sys.path.append(str(Path(__file__).resolve().parents[1]))

import bot


class DummyBot:
    def __init__(self) -> None:
        self.sent_messages: list[tuple[int, str, ParseMode | None]] = []

    async def send_message(self, chat_id: int, text: str, parse_mode: ParseMode | None = None) -> None:
        self.sent_messages.append((chat_id, text, parse_mode))


class DummyApplication:
    def __init__(self) -> None:
        self.bot = DummyBot()


def _run(coro: Awaitable[Any]) -> Any:
    return asyncio.run(coro)


@pytest.fixture
def db_path(monkeypatch, tmp_path):
    path = tmp_path / "bot.sqlite"
    monkeypatch.setattr(bot, "DB_PATH", str(path))
    _run(bot.init_db())
    return str(path)


async def _insert_subscriber(db_path: str, chat_id: int) -> None:
    async with aiosqlite.connect(db_path) as db:
        await db.execute("INSERT INTO subscriber(chat_id) VALUES (?)", (chat_id,))
        await db.commit()


async def _insert_signature(
    db_path: str,
    *,
    name: str,
    kind: str,
    expiry: date,
    note: str | None = None,
) -> None:
    async with aiosqlite.connect(db_path) as db:
        cur = await db.execute("INSERT INTO entity(name, kind) VALUES (?, ?)", (name, kind))
        entity_id = cur.lastrowid
        await db.execute(
            "INSERT INTO signature(entity_id, expiry, note, active) VALUES (?,?,?,1)",
            (entity_id, expiry.isoformat(), note),
        )
        await db.commit()


def test_send_reminders_for_upcoming_date(db_path):
    _run(_insert_subscriber(db_path, 101))
    expiry = date.today() + timedelta(days=5)
    _run(
        _insert_signature(
            db_path,
            name="ООО Ромашка",
            kind="org",
            expiry=expiry,
            note="Подпись_основная",
        )
    )
    app = DummyApplication()

    _run(bot.send_reminders(app))

    assert len(app.bot.sent_messages) == 1
    chat_id, text, parse_mode = app.bot.sent_messages[0]
    assert chat_id == 101
    assert "⏰ Напоминание: через 5 дн." in text
    assert "[ЮЛ] ООО Ромашка" in text
    assert expiry.strftime("%d.%m.%Y") in text
    assert "Подпись\\_основная" in text
    assert parse_mode == ParseMode.MARKDOWN


def test_send_reminders_for_today(db_path):
    _run(_insert_subscriber(db_path, 202))
    expiry = date.today()
    _run(
        _insert_signature(
            db_path,
            name="ИП Иванов",
            kind="person",
            expiry=expiry,
            note=None,
        )
    )
    app = DummyApplication()

    _run(bot.send_reminders(app))

    assert len(app.bot.sent_messages) == 1
    _, text, _ = app.bot.sent_messages[0]
    assert text.startswith("⚠️ Истекает сегодня!")
    assert "[ФЛ] ИП Иванов" in text


def test_send_reminders_ignores_non_target_dates(db_path):
    _run(_insert_subscriber(db_path, 303))
    expiry = date.today() + timedelta(days=3)
    _run(
        _insert_signature(
            db_path,
            name="ООО Лайм",
            kind="org",
            expiry=expiry,
            note="Тест",
        )
    )
    app = DummyApplication()

    _run(bot.send_reminders(app))

    assert app.bot.sent_messages == []


def test_send_reminders_orders_by_expiry(db_path):
    _run(_insert_subscriber(db_path, 404))
    later = date.today() + timedelta(days=25)
    sooner = date.today() + timedelta(days=10)
    _run(_insert_signature(db_path, name="ООО Альфа", kind="org", expiry=later, note=None))
    _run(_insert_signature(db_path, name="ИП Бета", kind="person", expiry=sooner, note=None))
    app = DummyApplication()

    _run(bot.send_reminders(app))

    messages = [text for _, text, _ in app.bot.sent_messages]
    assert len(messages) == 2
    assert "через 10 дн." in messages[0]
    assert "через 25 дн." in messages[1]
