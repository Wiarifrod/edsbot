import asyncio
import os
import time
from datetime import date, datetime, timedelta
from dateutil import parser as dateparser

import aiosqlite
from dotenv import load_dotenv

from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("edsbot")

# ====== ENV ======
load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}
TZ = os.getenv("TZ", "Europe/Riga")
REMIND_AT = os.getenv("REMIND_AT", "09:00")

os.environ["TZ"] = TZ
try:
    time.tzset()  # работает в Linux
except Exception:
    pass

DB_PATH = "data.db"

# --- Reply-кнопки и подменю ---
BTN_BACK = "⬅️ Назад"

# Кнопки главного меню
BTN_INFO   = "ℹ️ Информация"
BTN_ADD    = "➕ Добавление"
BTN_EDIT   = "✏️ Изменение"
BTN_DELETE = "🗑️ Удаление"
BTN_BROWSE = "📂 База подписей"

# Telegram может присылать текст кнопок с эмодзи, если пользователь
# нажал старую раскладку или скопировал подписи. Приводим такие варианты
# к каноничным именам, чтобы сравнения вида `text == BTN_INFO` продолжали
# работать.
# Подменю «Информация»
BTN_INFO_LAST10 = "🔟 Ближайшие 10"
BTN_INFO_LAST30 = "📆 Ближайшие 30"
BTN_INFO_ALL = "📋 Список всех"

# Подменю «Добавление»
BTN_ADD_SIGN = "🖊️ Добавить подпись"
BTN_ADD_REG  = "🆕 Добавить юр/фл в реестр"

# Выбор типа субъекта
BTN_KIND_ORG    = "🏢 Юр. лицо"
BTN_KIND_PERSON = "👤 Физ. лицо"

# Подменю «Удаление»
BTN_DELETE_SIGN = "🧾 Удалить запись"
BTN_DELETE_REG  = "🚮 Удалить из реестра"

BTN_ALIASES = {
    "Назад": BTN_BACK,
    "Информация": BTN_INFO,
    "Добавление": BTN_ADD,
    "Изменение": BTN_EDIT,
    "Удаление": BTN_DELETE,
    "База подписей": BTN_BROWSE,
    "Ближайшие 10": BTN_INFO_LAST10,
    "Ближайшие 30": BTN_INFO_LAST30,
    "Список всех": BTN_INFO_ALL,
    "Добавить подпись": BTN_ADD_SIGN,
    "Добавить юр/фл в реестр": BTN_ADD_REG,
    "Юр. лицо": BTN_KIND_ORG,
    "Физ. лицо": BTN_KIND_PERSON,
    "Удалить подпись": BTN_DELETE_SIGN,
    "Удалить запись": BTN_DELETE_SIGN,
    "🧾 Удалить запись": BTN_DELETE_SIGN,
    "🧾 Удалить подпись": BTN_DELETE_SIGN,
    "🗑️ Удалить подпись": BTN_DELETE_SIGN,
    "🗑️ Удалить запись": BTN_DELETE_SIGN,
    "Удалить из реестра": BTN_DELETE_REG,
    "🗑️ Удалить из реестра": BTN_DELETE_REG,
    "🚮 Удалить запись": BTN_DELETE_SIGN,
    "🚮 Удалить из реестра": BTN_DELETE_REG,
}

# Информация
CB_INFO_LAST10 = "info:last10"
CB_INFO_ALL = "info:all"

# Добавление
CB_ADD_SKIP_NOTE = "add:skip_note"

# Изменение
CB_UPD_SKIP_NOTE = "upd:skip_note"

# Удаление
CB_DEL_CONFIRM = "del:confirm"

# Удаление из реестра (второй пункт третьего блока)
CB_REGDEL_CONFIRM = "regdel:confirm"

TREE_CB_PREFIX = "tree|"


# безопасный «невидимый» символ, который Телеграм принимает как непустой текст
SAFE_EMPTY = "\u2063"  # Invisible Separator

# набор всех «зарезервированных» названий кнопок-реплаев,
# которые нельзя сохранять как примечание
RESERVED_BTNS = {
    BTN_INFO, BTN_ADD, BTN_EDIT, BTN_DELETE, BTN_BROWSE, BTN_BACK,
    BTN_INFO_LAST10, BTN_INFO_LAST30, BTN_INFO_ALL,
    BTN_ADD_SIGN, BTN_ADD_REG, BTN_KIND_ORG, BTN_KIND_PERSON,
    BTN_DELETE_SIGN, BTN_DELETE_REG,
}

MENU_BTNS = set(RESERVED_BTNS)

# Базовая иерархия организаций (может расширяться в будущем)
ORG_STRUCTURE: dict[str, dict] = {
    "Администрация района": {
        "Нагорское поселение": {},
        "Чеглаковское поселение": {},
    },
    "Управление образования": {
        "Школа с. Мулино": {},
    },
    "Управление культуры": {
        "РЦНТ": {},
        "ЦБС": {},
    },
}

# ====== HELPERS ======

def main_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_INFO)],
            [KeyboardButton(BTN_ADD), KeyboardButton(BTN_EDIT)],
            [KeyboardButton(BTN_DELETE)],
            [KeyboardButton(BTN_BROWSE)],
        ],
        resize_keyboard=True
    )

def info_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_INFO_LAST10), KeyboardButton(BTN_INFO_LAST30)],
            [KeyboardButton(BTN_INFO_ALL)],
            [KeyboardButton(BTN_BACK)],
        ], resize_keyboard=True
    )

def add_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_ADD_SIGN)],
            [KeyboardButton(BTN_ADD_REG)],
            [KeyboardButton(BTN_BACK)],
        ], resize_keyboard=True
    )

def kind_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_KIND_ORG), KeyboardButton(BTN_KIND_PERSON)],
            [KeyboardButton(BTN_BACK)],
        ], resize_keyboard=True
    )

def delete_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_DELETE_SIGN)],
            [KeyboardButton(BTN_DELETE_REG)],
            [KeyboardButton(BTN_BACK)],
        ], resize_keyboard=True
    )

def safe_md(text: str) -> str:
    return text.replace("_", "\\_").replace("*", "\\*")

def fmt_entity_row(r) -> str:
    # r: (id, name, kind) OR joined result with expiry/note
    kind = "ЮЛ" if r["kind"] == "org" else "ФЛ"
    return f"[{kind}] {r['name']}"

def fmt_signature_row(r) -> str:
    # r: joined entity+signature
    kind = "ЮЛ" if r["kind"] == "org" else "ФЛ"
    exp = r["expiry"]
    note = r["note"]
    today = date.today()
    exp_d = datetime.strptime(exp, "%Y-%m-%d").date()
    suffix = ""
    if exp_d < today:
        suffix = " — *истёкла*"
    elif exp_d == today:
        suffix = " — *сегодня!*"
    line = f"[{kind}] {r['name']} — до {exp_d.strftime('%d.%m.%Y')}{suffix}"
    if note:
        line += f"\n  Примечание: {safe_md(note)}"
    return line

async def ensure_group(db, name: str, parent_id: int | None) -> int:
    async with db.execute("SELECT id, parent_id FROM grp WHERE name=?", (name,)) as cur:
        row = await cur.fetchone()
    if row:
        gid = row["id"]
        if row["parent_id"] != parent_id:
            await db.execute("UPDATE grp SET parent_id=? WHERE id=?", (parent_id, gid))
        return gid
    cur = await db.execute("INSERT INTO grp(name, parent_id) VALUES (?,?)", (name, parent_id))
    return cur.lastrowid

async def ensure_org_entity(db, group_id: int, name: str) -> int:
    async with db.execute("SELECT id, kind, group_id FROM entity WHERE name=?", (name,)) as cur:
        row = await cur.fetchone()
    if row:
        eid = row["id"]
        if row["kind"] != "org":
            await db.execute("UPDATE entity SET kind='org' WHERE id=?", (eid,))
        if row["group_id"] != group_id:
            await db.execute("UPDATE entity SET group_id=? WHERE id=?", (group_id, eid))
        return eid
    try:
        cur = await db.execute(
            "INSERT INTO entity(name, kind, group_id) VALUES (?,?,?)",
            (name, "org", group_id)
        )
        return cur.lastrowid
    except aiosqlite.IntegrityError:
        async with db.execute("SELECT id FROM entity WHERE name=?", (name,)) as cur2:
            row2 = await cur2.fetchone()
        if not row2:
            raise
        eid = row2["id"]
        await db.execute(
            "UPDATE entity SET kind='org', group_id=? WHERE id=?",
            (group_id, eid)
        )
        return eid

async def ensure_org_structure(db, structure: dict[str, dict], parent_id: int | None = None):
    for name, children in structure.items():
        gid = await ensure_group(db, name, parent_id)
        await ensure_org_entity(db, gid, name)
        if children:
            await ensure_org_structure(db, children, gid)

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys = ON;")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS subscriber (
            chat_id INTEGER PRIMARY KEY
        );""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS entity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL CHECK(kind IN ('org','person')),
            group_id INTEGER NULL
        );""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS signature (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER NOT NULL,
            expiry TEXT NOT NULL,      -- YYYY-MM-DD
            note TEXT,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(entity_id) REFERENCES entity(id) ON DELETE CASCADE
        );""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS grp (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            parent_id INTEGER NULL,
            FOREIGN KEY(parent_id) REFERENCES grp(id) ON DELETE SET NULL
        );""")
        await ensure_org_structure(db, ORG_STRUCTURE)
        await db.commit()

async def is_allowed(user_id: int) -> bool:
    return (not ADMIN_IDS) or (user_id in ADMIN_IDS)

def parse_date(s: str) -> date:
    s = s.strip()
    # поддержим dd.mm.yyyy и yyyy-mm-dd
    try:
        if "." in s and len(s) >= 8:
            d = datetime.strptime(s, "%d.%m.%Y").date()
        else:
            # доверим dateutil любым нормальным строкам
            d = dateparser.parse(s, dayfirst=True).date()
        return d
    except Exception:
        raise ValueError("Неверный формат даты. Введите в виде 31.12.2025 или 2025-12-31")

async def upsert_signature(db, entity_id: int, expiry: date, note: str | None):
    await db.execute("PRAGMA foreign_keys = ON;")
    async with db.execute("SELECT id FROM signature WHERE entity_id=? AND active=1", (entity_id,)) as cur:
        row = await cur.fetchone()
    if row:
        sig_id = row[0]
        await db.execute(
            "UPDATE signature SET expiry=?, note=?, updated_at=datetime('now') WHERE id=?",
            (expiry.isoformat(), note, sig_id)
        )
    else:
        await db.execute(
            "INSERT INTO signature(entity_id, expiry, note, active) VALUES (?,?,?,1)",
            (entity_id, expiry.isoformat(), note)
        )
    await db.commit()

async def get_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT chat_id FROM subscriber") as cur:
            return [r[0] for r in await cur.fetchall()]

async def ensure_subscriber(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscriber(chat_id) VALUES (?)", (chat_id,))
        await db.commit()

async def get_group(group_id: int) -> aiosqlite.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, name, parent_id FROM grp WHERE id=?", (group_id,)) as cur:
            return await cur.fetchone()

async def list_groups(parent_id: int | None) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if parent_id is None:
            sql = "SELECT id, name FROM grp WHERE parent_id IS NULL ORDER BY name"
            args = ()
        else:
            sql = "SELECT id, name FROM grp WHERE parent_id=? ORDER BY name"
            args = (parent_id,)
        async with db.execute(sql, args) as cur:
            return await cur.fetchall()

async def get_group_legal_entity(group_id: int) -> aiosqlite.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, kind FROM entity WHERE group_id=? AND kind='org'",
            (group_id,)
        ) as cur:
            return await cur.fetchone()

async def list_group_persons(group_id: int) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, name, kind FROM entity WHERE group_id=? AND kind='person' ORDER BY lower(name)",
            (group_id,)
        ) as cur:
            return await cur.fetchall()

async def get_entity_with_signature(entity_id: int) -> aiosqlite.Row | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT e.id, e.name, e.kind, s.expiry, s.note
            FROM entity e
            LEFT JOIN signature s ON s.entity_id=e.id AND s.active=1
            WHERE e.id=?
            """,
            (entity_id,)
        ) as cur:
            return await cur.fetchone()

async def list_persons_with_signatures(group_id: int) -> list[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT e.id, e.name, e.kind, s.expiry, s.note
            FROM entity e
            LEFT JOIN signature s ON s.entity_id=e.id AND s.active=1
            WHERE e.group_id=? AND e.kind='person'
            ORDER BY lower(e.name)
            """,
            (group_id,)
        ) as cur:
            return await cur.fetchall()


# ---- TREE NAVIGATION ----

def _tree_cb(mode: str, action: str, payload: str = "_") -> str:
    return f"{TREE_CB_PREFIX}{mode}|{action}|{payload}"


def _tree_state(context: ContextTypes.DEFAULT_TYPE) -> dict | None:
    return context.user_data.get("tree")


def _tree_current(state: dict) -> tuple[int, str] | None:
    path: list[tuple[int, str]] = state.get("path", [])
    if not path:
        return None
    return path[-1]


def _tree_path_text(state: dict) -> str:
    path: list[tuple[int, str]] = state.get("path", [])
    if not path:
        return ""
    return " / ".join(safe_md(name) for _, name in path)


async def tree_start(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str):
    state = {"mode": mode, "path": []}
    if mode == "browse":
        state["view"] = "groups"
    context.user_data["tree"] = state
    text, markup = await build_tree_view(state)
    msg = await update.message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.MARKDOWN)
    state["message_id"] = msg.message_id
    state["chat_id"] = msg.chat.id


async def build_tree_view(state: dict) -> tuple[str, InlineKeyboardMarkup]:
    mode = state.get("mode")
    if mode == "browse":
        return await _build_tree_view_browse(state)
    return await _build_tree_view_picker(state)


async def _build_tree_view_browse(state: dict) -> tuple[str, InlineKeyboardMarkup]:
    path = state.get("path", [])
    current = _tree_current(state)
    group_id = current[0] if current else None
    view = state.get("view", "groups")
    if view not in {"groups", "employees", "legal"}:
        view = "groups"
        state["view"] = "groups"

    buttons: list[list[InlineKeyboardButton]] = []
    lines: list[str] = []

    if not path:
        lines.append("*База подписей*")
    else:
        lines.append(f"*{safe_md(current[1])}*")

    if view == "groups":
        if not path:
            lines.append("Выберите организацию.")
        else:
            lines.append("Выберите действие или подразделение.")

        children = await list_groups(group_id)
        if group_id is not None:
            legal = await get_group_legal_entity(group_id)
            if legal:
                buttons.append([
                    InlineKeyboardButton("📄 Подпись юридического лица", _tree_cb("browse", "show", "legal"))
                ])
            buttons.append([
                InlineKeyboardButton("👥 Сотрудники", _tree_cb("browse", "show", "employees"))
            ])
        for child in children:
            buttons.append([
                InlineKeyboardButton(f"🏢 {child['name']}", _tree_cb("browse", "enter", str(child["id"])))
            ])
        if path:
            buttons.append([InlineKeyboardButton("⬅️ Назад", _tree_cb("browse", "up"))])
        else:
            buttons.append([InlineKeyboardButton("🏠 Главное меню", _tree_cb("browse", "exit"))])
        return "\n".join(lines), InlineKeyboardMarkup(buttons)

    if group_id is None:
        state["view"] = "groups"
        return await _build_tree_view_browse(state)

    if view == "employees":
        rows = await list_persons_with_signatures(group_id)
        if rows:
            lines.append("Сотрудники:")
            for r in rows:
                if r["expiry"]:
                    lines.append(fmt_signature_row(r))
                else:
                    lines.append(f"[ФЛ] {safe_md(r['name'])} — подпись не заведена")
        else:
            lines.append("Сотрудников пока нет.")
    elif view == "legal":
        entity = await get_group_legal_entity(group_id)
        if entity:
            row = await get_entity_with_signature(entity["id"])
            if row and row["expiry"]:
                lines.append(fmt_signature_row(row))
            else:
                lines.append(f"[ЮЛ] {safe_md(entity['name'])} — подпись не заведена")
        else:
            lines.append("Для этой организации не заведено юридическое лицо.")

    buttons.append([InlineKeyboardButton("⬅️ Назад", _tree_cb("browse", "show", "groups"))])
    if not path:
        buttons.append([InlineKeyboardButton("🏠 Главное меню", _tree_cb("browse", "exit"))])
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def _build_tree_view_picker(state: dict) -> tuple[str, InlineKeyboardMarkup]:
    mode = state.get("mode")
    path = state.get("path", [])
    current = _tree_current(state)
    group_id = current[0] if current else None
    buttons: list[list[InlineKeyboardButton]] = []

    headers = {
        "sign_add_org": "Добавление подписи юридического лица",
        "sign_add_person": "Добавление подписи сотрудника",
        "sign_update": "Изменение подписи",
        "sign_delete": "Удаление подписи",
        "reg_delete": "Удаление из реестра",
        "reg_add_person": "Добавление сотрудника в реестр",
    }
    header = headers.get(mode, "Выбор организации")

    lines = [f"*{safe_md(header)}*"]
    if path:
        lines.append(f"Текущая организация: {safe_md(current[1])}")
    else:
        lines.append("Выберите организацию.")

    children = await list_groups(group_id)

    if mode == "reg_add_person":
        if current:
            buttons.append([
                InlineKeyboardButton(
                    "➕ Добавить сотрудника сюда",
                    _tree_cb(mode, "add", str(group_id))
                )
            ])
        for child in children:
            buttons.append([
                InlineKeyboardButton(f"🏢 {child['name']}", _tree_cb(mode, "enter", str(child["id"])))
            ])
    else:
        show_legal = mode in {"sign_add_org", "sign_update", "sign_delete", "reg_delete"}
        show_persons = mode in {"sign_add_person", "sign_update", "sign_delete", "reg_delete"}

        if current and show_legal:
            legal = await get_group_legal_entity(group_id)
            if legal:
                label = f"🏢 {legal['name']} (ЮЛ)"
                buttons.append([
                    InlineKeyboardButton(label, _tree_cb(mode, "select", str(legal["id"])))
                ])
        if current and show_persons:
            persons = await list_group_persons(group_id)
            for person in persons:
                label = f"👤 {person['name']}"
                buttons.append([
                    InlineKeyboardButton(label, _tree_cb(mode, "select", str(person["id"])))
                ])
        for child in children:
            buttons.append([
                InlineKeyboardButton(f"🏢 {child['name']}", _tree_cb(mode, "enter", str(child["id"])))
            ])

    if path:
        buttons.append([InlineKeyboardButton("⬅️ Назад", _tree_cb(mode, "up"))])
    buttons.append([InlineKeyboardButton("🏠 Главное меню", _tree_cb(mode, "exit"))])

    return "\n".join(lines), InlineKeyboardMarkup(buttons)


async def tree_handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, mode: str, action: str, payload: str):
    q = update.callback_query
    await q.answer()
    state = _tree_state(context)
    if not state or state.get("mode") != mode:
        state = {"mode": mode, "path": []}
        if mode == "browse":
            state["view"] = "groups"
        context.user_data["tree"] = state

    if action == "exit":
        context.user_data.pop("tree", None)
        await q.edit_message_text("Возврат в главное меню…")
        await _go_main(context, q.message.chat.id)
        return

    if action == "up":
        path: list[tuple[int, str]] = state.get("path", [])
        if path:
            path.pop()
        if mode == "browse":
            state["view"] = "groups"
        text, markup = await build_tree_view(state)
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        return

    if action == "enter":
        group_id = int(payload)
        row = await get_group(group_id)
        if not row:
            await q.answer("Организация не найдена")
            return
        path: list[tuple[int, str]] = state.setdefault("path", [])
        path.append((group_id, row["name"]))
        if mode == "browse":
            state["view"] = "groups"
        text, markup = await build_tree_view(state)
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        return

    if mode == "browse" and action == "show":
        state["view"] = payload
        text, markup = await build_tree_view(state)
        await q.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=markup)
        return

    if mode == "reg_add_person" and action == "add":
        group_id = int(payload)
        row = await get_group(group_id)
        if not row:
            await q.answer("Организация не найдена")
            return
        context.user_data["awaiting"] = "new_entity_name"
        context.user_data["kind"] = "person"
        context.user_data["add_action"] = "reg"
        context.user_data["group_id"] = group_id
        context.user_data.pop("tree", None)
        await q.edit_message_text(
            f"Введите полное имя сотрудника для организации «{safe_md(row['name'])}».",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    if action == "select":
        entity_id = int(payload)
        row = await get_entity_with_signature(entity_id)
        if not row:
            await q.answer("Запись не найдена")
            return
        context.user_data.pop("tree", None)
        if mode == "sign_add_org":
            context.user_data["entity_id"] = entity_id
            context.user_data["entity_kind"] = "org"
            context.user_data["flow"] = "add"
            context.user_data["awaiting"] = "expiry"
            await q.edit_message_text(
                f"Выбрана организация: {safe_md(row['name'])}.\nВведите дату окончания подписи (например 31.12.2025).",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if mode == "sign_add_person":
            context.user_data["entity_id"] = entity_id
            context.user_data["entity_kind"] = "person"
            context.user_data["flow"] = "add"
            context.user_data["awaiting"] = "expiry"
            await q.edit_message_text(
                f"Выбран сотрудник: {safe_md(row['name'])}.\nВведите дату окончания подписи (например 31.12.2025).",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if mode == "sign_update":
            context.user_data["entity_id"] = entity_id
            context.user_data["entity_kind"] = row["kind"]
            context.user_data["flow"] = "upd"
            context.user_data["awaiting"] = "expiry"
            await q.edit_message_text(
                f"Выбрана запись: {safe_md(row['name'])}.\nВведите новую дату окончания подписи.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        if mode == "sign_delete":
            await show_and_confirm_delete(q, entity_id)
            return
        if mode == "reg_delete":
            await show_and_confirm_regdelete(q, entity_id)
            return

    await q.answer("Действие недоступно")

# ====== HANDLERS ======

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id):
        await update.message.reply_text("Доступ запрещён.", quote=True)
        return
    await ensure_subscriber(update.effective_chat.id)
    txt = (
        "Привет! Я бот мониторинга ЭЦП.\n\n"
        "• Веду реестр организаций и физлиц\n"
        "• Храню срок действия подписи + примечание\n"
        "• Показываю ближайшие истечения\n"
        "• Напоминаю за 25/20/15/10/5 дней\n\n"
        "Выберите действие кнопками ниже."
    )
    await update.message.reply_text(txt, reply_markup=main_menu_kbd())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id):
        return
    await update.message.reply_text(
        "/start — меню\n"
        "/add — добавить запись подписи\n"
        "/update — изменить запись\n"
        "/delete — удалить запись подписи\n"
        "/registry_delete — удалить из реестра (и связанные записи)\n"
        "/all — список всех\n"
        "/next — ближайшие 10\n"
        "Подсказки работают кнопками после ввода первых букв.",
        reply_markup=main_menu_kbd()
    )

# ---- INFO BLOCK ----

def info_inline_kbd():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ближайшие 10", callback_data=CB_INFO_LAST10)],
        [InlineKeyboardButton("Список всех", callback_data=CB_INFO_ALL)],
    ])

async def info_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id):
        return
    # Входим в подменю «Информация»
    context.user_data["menu"] = "info"
    await update.message.reply_text("Что показать?", reply_markup=info_menu_kbd())

async def cb_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id):
        return
    q = update.callback_query
    await q.answer()
    if q.data == CB_INFO_LAST10:
        txt = await build_last10_text()
    else:
        txt = await build_all_text()
    await q.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)

async def build_last10_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        today = date.today().isoformat()
        sql = """
        SELECT e.id, e.name, e.kind, s.expiry, s.note
        FROM signature s
        JOIN entity e ON e.id = s.entity_id
        WHERE s.active=1 AND date(s.expiry) >= date(?)
        ORDER BY date(s.expiry) ASC
        LIMIT 10;
        """
        async with db.execute(sql, (today,)) as cur:
            rows = await cur.fetchall()
    if not rows:
        return "Нет предстоящих окончаний."
    lines = ["*Ближайшие 10:*"]
    for r in rows:
        lines.append(fmt_signature_row(r))
    return "\n".join(lines)

async def build_lastN_text(limit: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        today = date.today().isoformat()
        sql = f"""
        SELECT e.id, e.name, e.kind, s.expiry, s.note
        FROM signature s
        JOIN entity e ON e.id = s.entity_id
        WHERE s.active=1 AND date(s.expiry) >= date(?)
        ORDER BY date(s.expiry) ASC
        LIMIT {limit};
        """
        async with db.execute(sql, (today,)) as cur:
            rows = await cur.fetchall()
    if not rows:
        return "Нет предстоящих окончаний."
    title = f"*Ближайшие {limit}:*"
    lines = [title] + [fmt_signature_row(r) for r in rows]
    return "\n".join(lines)

async def build_all_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
        SELECT e.id, e.name, e.kind, s.expiry, s.note
        FROM entity e
        LEFT JOIN signature s ON s.entity_id=e.id AND s.active=1
        ORDER BY CASE WHEN e.kind='org' THEN 0 ELSE 1 END, lower(e.name);
        """
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()
    if not rows:
        return "Реестр пуст."
    lines = ["*Список всех:* (сначала ЮЛ, потом ФЛ)"]
    for r in rows:
        if r["expiry"]:
            lines.append(fmt_signature_row(r))
        else:
            kind = "ЮЛ" if r["kind"] == "org" else "ФЛ"
            lines.append(f"[{kind}] {r['name']} — подпись не заведена")
    return "\n".join(lines)

# Команды-ярлыки
async def cmd_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    await update.message.reply_text(await build_all_text(), parse_mode=ParseMode.MARKDOWN)

async def cmd_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    await update.message.reply_text(await build_last10_text(), parse_mode=ParseMode.MARKDOWN)

# ---- ADD / UPDATE / DELETE FLOWS ----

async def add_entry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id):
        return
    context.user_data.clear()
    context.user_data["menu"] = "add_menu"
    await update.message.reply_text("Выберите вариант:", reply_markup=add_menu_kbd())


async def upd_entry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    context.user_data.clear()
    await tree_start(update, context, "sign_update")

async def del_entry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    context.user_data.clear()
    context.user_data["menu"] = "delete"
    await update.message.reply_text(
        "Что удаляем?",
        reply_markup=delete_menu_kbd()
    )

async def regdel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    context.user_data.clear()
    await tree_start(update, context, "reg_delete")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting"):
        return
    if not await is_allowed(update.effective_user.id):
        return

    text = update.message.text.strip().replace("\u00a0", " ")
    text = BTN_ALIASES.get(text, text)

    if text == BTN_BACK:
        await _go_main(context, update.effective_chat.id)
        return

    # --- Подменю «Информация» ---
    if context.user_data.get("menu") == "info":
        if text == BTN_BACK:
            context.user_data.pop("menu", None)
            await _go_main(context, update.effective_chat.id)
            return
        if text == BTN_INFO_LAST10:
            await update.message.reply_text(await build_lastN_text(10), parse_mode=ParseMode.MARKDOWN)
            return
        if text == BTN_INFO_LAST30:
            await update.message.reply_text(await build_lastN_text(30), parse_mode=ParseMode.MARKDOWN)
            return
        if text == BTN_INFO_ALL:
            await update.message.reply_text(await build_all_text(), parse_mode=ParseMode.MARKDOWN)
            return
        return

    # --- Подменю «Добавление» ---
    if context.user_data.get("menu") == "add_menu":
        if text == BTN_BACK:
            context.user_data.clear()
            await _go_main(context, update.effective_chat.id)
            return
        if text == BTN_ADD_SIGN:
            context.user_data["add_action"] = "sign"
            context.user_data["menu"] = "add_pick_kind"
            await update.message.reply_text("Кого добавляем подпись?", reply_markup=kind_menu_kbd())
            return
        if text == BTN_ADD_REG:
            context.user_data["add_action"] = "reg"
            context.user_data["menu"] = "add_pick_kind"
            await update.message.reply_text("Кого добавить в реестр?", reply_markup=kind_menu_kbd())
            return
        return

    # --- Подменю «Удаление» ---
    if context.user_data.get("menu") == "delete":
        if text == BTN_BACK:
            context.user_data.clear()
            await _go_main(context, update.effective_chat.id)
            return
        if text == BTN_DELETE_SIGN:
            context.user_data.clear()
            await tree_start(update, context, "sign_delete")
            return
        if text == BTN_DELETE_REG:
            context.user_data.clear()
            await tree_start(update, context, "reg_delete")
            return
        return

    # --- Подменю «Выбор типа субъекта» ---
    if context.user_data.get("menu") == "add_pick_kind":
        if text == BTN_BACK:
            context.user_data["menu"] = "add_menu"
            await update.message.reply_text("Выберите вариант:", reply_markup=add_menu_kbd())
            return
        if text == BTN_KIND_ORG:
            kind = "org"
        elif text == BTN_KIND_PERSON:
            kind = "person"
        else:
            return

        context.user_data["kind"] = kind
        action = context.user_data.get("add_action")
        context.user_data.pop("menu", None)

        if action == "sign":
            if kind == "org":
                await tree_start(update, context, "sign_add_org")
            else:
                await tree_start(update, context, "sign_add_person")
            return

        if action == "reg":
            if kind != "person":
                await update.message.reply_text("Юридические лица добавляются через код администратора.")
                return
            context.user_data["add_action"] = "reg"
            await tree_start(update, context, "reg_add_person")
            return
        return

    # --- Главное меню (фолбэк) ---
    if text == BTN_INFO:
        await info_block(update, context)
        return
    if text == BTN_ADD:
        await add_entry_cmd(update, context)
        return
    if text == BTN_EDIT:
        await upd_entry_cmd(update, context)
        return
    if text == BTN_DELETE:
        await del_entry_cmd(update, context)
        return
    if text == BTN_BROWSE:
        context.user_data.clear()
        await tree_start(update, context, "browse")
        return

async def on_text_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает шаги ввода: имя новой сущности, дата, примечание."""
    if not await is_allowed(update.effective_user.id):
        return
    ud = context.user_data

    # Сначала получаем msg
    msg = update.message.text.strip() if update.message and update.message.text else ""
    msg = BTN_ALIASES.get(msg, msg)

    # Глобальный "Назад" — всегда в главное меню
    if msg == BTN_BACK:
        await _go_main(context, update.effective_chat.id)
        return
    if ud.get("awaiting") == "note" and msg in MENU_BTNS:
        await update.message.reply_text(
            "Сначала введите примечание текстом или нажмите «Пропустить».",
        )
        return

    awaiting = ud.get("awaiting")
    if not awaiting:
        return  # обычный текст ловит on_text

    # --- Создание новой сущности в реестре ---
    if awaiting == "new_entity_name":
        name = msg
        kind = ud.get("kind", "org")
        group_id = ud.get("group_id")
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                if group_id is not None:
                    await db.execute(
                        "INSERT INTO entity(name, kind, group_id) VALUES (?,?,?)",
                        (name, kind, group_id)
                    )
                else:
                    await db.execute("INSERT INTO entity(name, kind) VALUES (?,?)", (name, kind))
                await db.commit()
            except aiosqlite.IntegrityError:
                await update.message.reply_text("Такая сущность уже есть в реестре.")
                return
            async with db.execute("SELECT id FROM entity WHERE name=?", (name,)) as cur:
                row = await cur.fetchone()
                ud["entity_id"] = int(row[0])

        if ud.get("add_action") == "reg":
            ent_kind = "ЮЛ" if kind == "org" else "ФЛ"
            if group_id is not None:
                group_row = await get_group(group_id)
                if group_row:
                    await update.message.reply_text(
                        f"✅ Добавлено в реестр: {ent_kind} {name}\n"
                        f"Организация: {safe_md(group_row['name'])}",
                        parse_mode=ParseMode.MARKDOWN
                    )
                else:
                    await update.message.reply_text(f"✅ Добавлено в реестр: {ent_kind} {name}")
            else:
                await update.message.reply_text(f"✅ Добавлено в реестр: {ent_kind} {name}")
            await _go_main(context, update.effective_chat.id)
            return

        ud["awaiting"] = "expiry"
        await update.message.reply_text(
            "Ок. Теперь введите дату окончания подписи (например 31.12.2025).",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True)
        )
        return

    # --- Ввод/обновление даты окончания ---
    if awaiting == "expiry":
        try:
            d = parse_date(msg)
        except ValueError as e:
            await update.message.reply_text(str(e))
            return
        ud["expiry"] = d
        if ud.get("entity_kind") == "org":
            ud.pop("awaiting", None)
            await finalize_save(update, context, None)
            return
        ud["awaiting"] = "note"
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "Пропустить",
                callback_data=CB_ADD_SKIP_NOTE if ud.get("flow", "add") == "add" else CB_UPD_SKIP_NOTE
            )
        ]])
        await update.message.reply_text(
            "Добавьте примечание (необязательно) и отправьте сообщением, или нажмите «Пропустить».",
            reply_markup=kb
        )
        return

    # --- Примечание ---
    if awaiting == "note":
        # если пользователь ткнул любую кнопку из реплаев — трактуем как «Пропустить»
        if msg in RESERVED_BTNS:
            await finalize_save(update, context, None)
            return
        # обычный текст — сохраняем как примечание
        note = msg if msg else None
        await finalize_save(update, context, note)
        return


async def cb_skip_note(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    await finalize_save(q, context, None)

async def finalize_save(update_or_cb, context: ContextTypes.DEFAULT_TYPE, note: str | None):
    ud = context.user_data
    entity_id = ud.get("entity_id")
    expiry: date = ud.get("expiry")
    flow = ud.get("flow", "add")
    if not entity_id or not expiry:
        if isinstance(update_or_cb, Update) and update_or_cb.message:
            await update_or_cb.message.reply_text("Не хватает данных для сохранения. Попробуйте заново /add.")
        else:
            await update_or_cb.edit_message_text("Не хватает данных для сохранения. Попробуйте заново /add.")
        return

    async with aiosqlite.connect(DB_PATH) as db:
        await upsert_signature(db, entity_id, expiry, note)
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT name, kind FROM entity WHERE id=?", (entity_id,)) as cur:
            ent = await cur.fetchone()

    kind = "ЮЛ" if ent["kind"]=="org" else "ФЛ"
    txt = (
        f"✅ Сохранено ({'добавлено' if flow=='add' else 'обновлено'}):\n"
        f"{kind} {ent['name']}\n"
        f"Срок: {expiry.strftime('%d.%m.%Y')}"
    )
    if note:
        txt += f"\nПримечание: {safe_md(note)}"

    if isinstance(update_or_cb, Update) and update_or_cb.message:
        # если пришло обычным сообщением — ответим и сразу вернём главное меню
        await update_or_cb.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)
        await _go_main(context, update_or_cb.effective_chat.id)
    else:
        # если это был callback — сначала правим исходное сообщение
        await update_or_cb.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN)
        # и отдельно шлём главное меню
        chat_id = update_or_cb.message.chat.id
        await _go_main(context, chat_id)

    # state уже очищен внутри _go_main


# ---- DELETE SIGNATURE ----

async def show_and_confirm_delete(cbq, entity_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT e.id, e.name, e.kind, s.expiry, s.note
            FROM entity e LEFT JOIN signature s ON s.entity_id=e.id AND s.active=1
            WHERE e.id=?""", (entity_id,)) as cur:
            r = await cur.fetchone()
    if not r or not r["expiry"]:
        await cbq.edit_message_text("У этой записи нет активной подписи.")
        return
    txt = "Удалить подпись?\n" + fmt_signature_row(r)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Удалить", callback_data=f"{CB_DEL_CONFIRM}:{entity_id}")],
        [InlineKeyboardButton("Отмена", callback_data="noop")]
    ])
    await cbq.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

async def cb_del_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    _, entity_id_str = q.data.split(":")
    eid = int(entity_id_str)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE signature SET active=0, updated_at=datetime('now') WHERE entity_id=? AND active=1", (eid,))
        await db.commit()
    await q.edit_message_text("🗑️ Подпись удалена.", reply_markup=None)
    await _go_main(context, q.message.chat.id)


# ---- DELETE FROM REGISTRY ----

async def show_and_confirm_regdelete(cbq, entity_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, name, kind FROM entity WHERE id=?", (entity_id,)) as cur:
            e = await cur.fetchone()
    if not e:
        await cbq.edit_message_text("Не найдено.")
        return
    txt = f"Удалить из реестра *вместе со всеми записями*?\n{fmt_entity_row(e)}"
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Удалить из реестра", callback_data=f"{CB_REGDEL_CONFIRM}:{entity_id}")],
        [InlineKeyboardButton("Отмена", callback_data="noop")]
    ])
    await cbq.edit_message_text(txt, parse_mode=ParseMode.MARKDOWN, reply_markup=kb)

async def cb_regdel_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    _, entity_id_str = q.data.split(":")
    eid = int(entity_id_str)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA foreign_keys = ON;")
        await db.execute("DELETE FROM entity WHERE id=?", (eid,))
        await db.commit()
    await q.edit_message_text("🗑️ Удалено из реестра вместе со связанными записями.")
    await _go_main(context, q.message.chat.id)


# ---- CALLBACK ROUTER ----

async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    data = q.data or ""
    if data.startswith("info:"):
        await cb_info(update, context); return
    if data.startswith(TREE_CB_PREFIX):
        parts = data.split("|", 3)
        while len(parts) < 4:
            parts.append("_")
        _, mode, action, payload = parts
        await tree_handle_callback(update, context, mode, action, payload)
        return
    if data.startswith(CB_DEL_CONFIRM):
        await cb_del_confirm(update, context); return
    if data.startswith(CB_REGDEL_CONFIRM):
        await cb_regdel_confirm(update, context); return
    if data in (CB_ADD_SKIP_NOTE, CB_UPD_SKIP_NOTE):
        await cb_skip_note(update, context); return
    if data == "noop":
        await q.answer("Отменено")
        return

# ---- SCHEDULER ----

async def send_reminders(application: Application, today_override: date | None = None):
    """Шлёт напоминания. Можно подменить 'сегодня' через today_override для тестов."""
    days_list = [25, 20, 15, 10, 5, 0]  # 0 = сегодня
    today = today_override or date.today()
    targets = {(today + timedelta(days=d)).isoformat(): d for d in days_list}

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        placeholders = ",".join([f"'{t}'" for t in targets.keys()])
        sql = f"""
        SELECT e.name, e.kind, s.expiry, s.note
        FROM signature s
        JOIN entity e ON e.id=s.entity_id
        WHERE s.active=1 AND date(s.expiry) IN ({placeholders})
        ORDER BY date(s.expiry) ASC, lower(e.name);
        """
        async with db.execute(sql) as cur:
            rows = await cur.fetchall()

    if not rows:
        return

    subs = await get_subscribers()
    for r in rows:
        exp = datetime.strptime(r["expiry"], "%Y-%m-%d").date()
        diff = (exp - today).days
        if diff not in days_list:  # safety
            continue
        kind = "ЮЛ" if r["kind"] == "org" else "ФЛ"
        if diff > 0:
            header = f"⏰ Напоминание: через {diff} дн."
        elif diff == 0:
            header = "⚠️ Истекает сегодня!"
        else:
            header = f"❗ Просрочено на {-diff} дн."

        msg = f"{header}\n[{kind}] {r['name']}\nСрок: {exp.strftime('%d.%m.%Y')}"
        if r["note"]:
            msg += f"\nПримечание: {safe_md(r['note'])}"

        for chat_id in subs:
            try:
                await application.bot.send_message(chat_id, msg, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass

def schedule_daily(application: Application):
    # Планируем отправку раз в сутки в REMIND_AT локального TZ
    h, m = map(int, REMIND_AT.split(":"))
    # Используем внутренний job_queue PTB
    application.job_queue.run_daily(
        lambda ctx: asyncio.create_task(send_reminders(application)),
        time=datetime.now().replace(hour=h, minute=m, second=0, microsecond=0).timetz()
    )

async def test_reminder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручной запуск рассылки.
    Использование:
      /test_reminder           — как есть, на реальную сегодняшнюю дату
      /test_reminder 5         — проверить как будто сегодня +5 дней (сработают записи на 0/5/10/15/20/25 от этой базы)
      /test_reminder -2        — сдвиг назад на 2 дня (для отладки 'сегодня' и 'просрочено')
    """
    if not await is_allowed(update.effective_user.id):
        return

    # читаем опциональный сдвиг, по умолчанию 0
    offset = 0
    if context.args:
        try:
            offset = int(context.args[0])
        except ValueError:
            await update.message.reply_text("Аргумент должен быть целым числом (например, /test_reminder 5).")
            return

    today_override = date.today() + timedelta(days=offset) if offset != 0 else None

    await update.message.reply_text("⏳ Запускаю проверку напоминаний…")
    await send_reminders(context.application, today_override=today_override)
    await update.message.reply_text("✅ Готово. Если нашлись подходящие записи, подписчики получили уведомления.")


async def _dbg_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        txt = update.message.text if update.message else None
        logger.info("DBG MSG: uid=%s chat=%s text=%r",
                    update.effective_user.id if update.effective_user else None,
                    update.effective_chat.id if update.effective_chat else None,
                    txt)
    except Exception as e:
        logger.exception("DBG MSG error: %s", e)

async def _dbg_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = update.callback_query.data if update.callback_query else None
        logger.info("DBG CB: uid=%s chat=%s data=%r",
                    update.effective_user.id if update.effective_user else None,
                    update.effective_chat.id if update.effective_chat else None,
                    data)
        if update.callback_query:
            await update.callback_query.answer()
    except Exception as e:
        logger.exception("DBG CB error: %s", e)

async def _go_main(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Тихо возвращает пользователя в главное меню, без лишнего текста."""
    await context.bot.send_message(chat_id, SAFE_EMPTY, reply_markup=main_menu_kbd())
    context.user_data.clear()


# ====== MAIN ======

def build_app() -> Application:
    app = Application.builder().token(TOKEN).build()

    # --- диагностические ловцы всего на свете ---
    app.add_handler(CallbackQueryHandler(_dbg_cb), group=99)
    app.add_handler(MessageHandler(filters.ALL, _dbg_msg), group=99)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("all", cmd_all))
    app.add_handler(CommandHandler("next", cmd_next))
    app.add_handler(CommandHandler("add", add_entry_cmd))
    app.add_handler(CommandHandler("update", upd_entry_cmd))
    app.add_handler(CommandHandler("delete", del_entry_cmd))
    app.add_handler(CommandHandler("registry_delete", regdel_cmd))
    app.add_handler(CommandHandler("test_reminder", test_reminder_cmd))

    app.add_handler(CallbackQueryHandler(cb_router))

    # Текстовые сообщения:
    # 1) шаги ввода внутри сценариев
    app.add_handler(MessageHandler(filters.TEXT & filters.User(user_id=list(ADMIN_IDS)), on_text_flow), group=0)
    app.add_handler(MessageHandler(filters.TEXT, on_text), group=1)

    return app

import asyncio as _a

async def _amain():
    if not TOKEN:
        raise SystemExit("Нет токена TELEGRAM_BOT_TOKEN в .env")

    # Инициализация БД
    await init_db()

    app = build_app()
    schedule_daily(app)

    # Инициализируем и запускаем приложение вручную (чистый async-путь для Py3.12)
    await app.initialize()
    await app.start()

    # На всякий случай — сброс вебхука
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    # Стартуем polling (это корутина в v21)
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    # Держим процесс живым
    try:
        await _a.Event().wait()
    finally:
        # Корректная остановка
        await app.updater.stop()  # на всякий — снимет long-poll
        await app.stop()
        await app.shutdown()

def main():
    # Единый вход: запускаем всю логику в одном event loop
    _a.run(_amain())

if __name__ == "__main__":
    main()





