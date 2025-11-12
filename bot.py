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

BTN_CREATE_CONFIRM = "Добавить в реестр"
BTN_CANCEL         = "Отмена"

# Кнопки главного меню
BTN_INFO  = "Информация"
BTN_ADD   = "Добавление"
BTN_EDIT  = "Изменение"
BTN_DELETE= "Удаление"

# Подменю «Информация»
BTN_INFO_LAST10 = "Ближайшие 10"
BTN_INFO_LAST30 = "Ближайшие 30"
BTN_INFO_ALL = "Список всех"

# Подменю «Добавление»
BTN_ADD_SIGN = "Добавить подпись"
BTN_ADD_REG  = "Добавить юр/фл в реестр"

# Выбор типа субъекта
BTN_KIND_ORG    = "Юр. лицо"
BTN_KIND_PERSON = "Физ. лицо"

# Информация
CB_INFO_LAST10 = "info:last10"
CB_INFO_ALL = "info:all"

# Добавление
CB_ADD_START = "add:start"
CB_ADD_KIND_ORG = "add:kind:org"
CB_ADD_KIND_PERSON = "add:kind:person"
CB_ADD_NEW_ENTITY = "add:new_entity"
CB_ADD_PICK_PAGE = "add:pick_page"
CB_ADD_SKIP_NOTE = "add:skip_note"

# Изменение
CB_UPD_START = "upd:start"
CB_UPD_PICK_PAGE = "upd:pick_page"
CB_UPD_SKIP_NOTE = "upd:skip_note"

# Удаление
CB_DEL_START = "del:start"
CB_DEL_PICK_PAGE = "del:pick_page"
CB_DEL_CONFIRM = "del:confirm"

# Удаление из реестра (второй пункт третьего блока)
CB_REGDEL_START = "regdel:start"
CB_REGDEL_PICK_PAGE = "regdel:pick_page"
CB_REGDEL_CONFIRM = "regdel:confirm"


# набор всех «зарезервированных» названий кнопок-реплаев,
# которые нельзя сохранять как примечание
RESERVED_BTNS = {
    BTN_INFO, BTN_ADD, BTN_EDIT, BTN_DELETE, BTN_BACK,
    BTN_INFO_LAST10, BTN_INFO_LAST30, BTN_INFO_ALL,
    BTN_ADD_SIGN, BTN_ADD_REG, BTN_KIND_ORG, BTN_KIND_PERSON,
    BTN_CREATE_CONFIRM, BTN_CANCEL,
}

# Используется для проверки, что пользователь не нажимает кнопки меню
# вместо ввода текста на определённых шагах.
MENU_BTNS = RESERVED_BTNS

# ====== HELPERS ======

def main_menu_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_INFO)],
            [KeyboardButton(BTN_ADD), KeyboardButton(BTN_EDIT)],
            [KeyboardButton(BTN_DELETE)],
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

def create_confirm_kbd() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_CREATE_CONFIRM), KeyboardButton(BTN_CANCEL)],
            [KeyboardButton(BTN_BACK)],
        ], resize_keyboard=True
    )

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

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

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
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

async def count_entities_by_prefix(kind: str | None, prefix: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        args = [prefix + "%"]
        where = "WHERE lower(name) LIKE lower(?)"
        if kind:
            where += " AND kind=?"
            args.append(kind)
        async with db.execute(f"SELECT COUNT(*) FROM entity {where};", args) as cur:
            (cnt,) = await cur.fetchone()
    return int(cnt)


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
    context.user_data["mode"] = "upd"
    await update.message.reply_text("Введите первые буквы названия и отправьте сообщением.\n"
                                    "Я пришлю список подходящих вариантов кнопками.")

async def del_entry_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    context.user_data.clear()
    context.user_data["mode"] = "del"
    await update.message.reply_text("Удаление записи подписи.\n"
                                    "Введите первые буквы названия — пришлю список.")

async def regdel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    context.user_data.clear()
    context.user_data["mode"] = "regdel"
    await update.message.reply_text("Удаление из реестра (и связанных записей).\n"
                                    "Введите первые буквы названия — пришлю список.")

async def add_pick_kind(update: Update, context: ContextTypes.DEFAULT_TYPE, kind: str):
    context.user_data["kind"] = kind
    await update.effective_message.reply_text(
        "Введите первые буквы названия (или полное имя) и отправьте.\n"
        "Если в реестре нет — предложу создать."
    )

async def cb_add_kind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    if q.data == CB_ADD_KIND_ORG:
        await add_pick_kind(update, context, "org")
    else:
        await add_pick_kind(update, context, "person")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("awaiting"):
        return
    if context.user_data.pop("_skip_next_on_text", False):
        return
    if not await is_allowed(update.effective_user.id):
        return

    # ВАЖНО: сначала получаем текст
    text = update.message.text.strip()

    if text == BTN_BACK:
        await _go_main(context, update.effective_chat.id, silent=True)
        return

    # --- Подменю «Информация» ---
    if context.user_data.get("menu") == "info":
        if text == BTN_BACK:
            await _go_main(context, update.effective_chat.id, silent=True)
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
        # неузнанный ввод в рамках меню — игнор
        return

    # --- Подменю «Добавление» (ОТДЕЛЬНЫЙ блок, не внутри «Информация») ---
    if context.user_data.get("menu") == "add_menu":
        if text == BTN_BACK:
            await _go_main(context, update.effective_chat.id, silent=True)
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

    # --- Подменю «Выбор типа субъекта» ---
    if context.user_data.get("menu") == "add_pick_kind":
        if text == BTN_BACK:
            context.user_data["menu"] = "add_menu"
            await update.message.reply_text("Выберите вариант:", reply_markup=add_menu_kbd())
            return
        if text == BTN_KIND_ORG:
            context.user_data["kind"] = "org"
        elif text == BTN_KIND_PERSON:
            context.user_data["kind"] = "person"
        else:
            return

        if context.user_data.get("add_action") == "sign":
            context.user_data["mode"] = "add"
            context.user_data["menu"] = "add_search"
            await update.message.reply_text(
                "Введите первые буквы/наименование субъекта. "
                "Покажу совпадения, а если не найдётся — предложу добавить.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True)
            )
            return

        if context.user_data.get("add_action") == "reg":
            context.user_data["awaiting"] = "new_entity_name"
            context.user_data["menu"] = "add_reg_name"
            await update.message.reply_text(
                "Введите полное наименование для добавления в реестр.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True)
            )
            return

    # --- Поиск по реестру / автодобавление (для «добавить подпись») ---
    if context.user_data.get("menu") == "add_search" and context.user_data.get("mode") == "add":
        if text == BTN_BACK:
            context.user_data["menu"] = "add_pick_kind"
            await update.message.reply_text("Кого добавляем?", reply_markup=kind_menu_kbd())
            return

        kind = context.user_data.get("kind")
        prefix = text
        context.user_data["prefix"] = prefix

        cnt = await count_entities_by_prefix(kind, prefix)
        if cnt == 0:
            context.user_data["awaiting"] = "confirm_create"
            context.user_data["proposed_name"] = prefix
            await update.message.reply_text(
                f"В реестре нет записей на «{safe_md(prefix)}».\nДобавить новую запись с этим именем?",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=create_confirm_kbd()
            )
            return

        await send_entity_choices(update, "add", prefix, page=0, kind=kind)
        return

    # --- Ввод полного наименования для «Добавить юр/фл в реестр» ---
    if context.user_data.get("menu") == "add_reg_name" and context.user_data.get("awaiting") == "new_entity_name":
        if text == BTN_BACK:
            context.user_data["menu"] = "add_pick_kind"
            context.user_data.pop("awaiting", None)
            await update.message.reply_text("Кого добавляем в реестр?", reply_markup=kind_menu_kbd())
            return
        # Здесь не return — пусть on_text_flow обработает awaiting="new_entity_name"

    # --- Главное меню (фолбэк) ---
    if text == BTN_INFO:
        await info_block(update, context); return
    if text == BTN_ADD:
        await add_entry_cmd(update, context); return
    if text == BTN_EDIT:
        await upd_entry_cmd(update, context); return
    if text == BTN_DELETE:
        await del_entry_cmd(update, context); return

    mode = context.user_data.get("mode")
    if mode in {"upd", "del", "regdel"}:
        prefix = text
        await send_entity_choices(update, mode, prefix, page=0)
        context.user_data["prefix"] = prefix
        return

    if context.user_data.get("mode") == "add":
        kind = context.user_data.get("kind")
        if not kind:
            await update.message.reply_text("Сначала выберите тип: нажмите «Добавление» → ЮЛ/ФЛ.")
            return
        prefix = text
        await send_entity_choices(update, "add", prefix, page=0, kind=kind)
        context.user_data["prefix"] = prefix
        return

async def send_entity_choices(update_or_cb, mode: str, prefix: str, page: int, kind: str | None = None):
    # Выдает список подходящих сущностей страницами по 10
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        args = []
        where = "WHERE lower(name) LIKE lower(?)"
        args.append(prefix + "%")
        if kind:
            where += " AND kind=?"
            args.append(kind)
        sql = f"SELECT id, name, kind FROM entity {where} ORDER BY lower(name);"
        async with db.execute(sql, args) as cur:
            rows = await cur.fetchall()
    total = len(rows)
    page_size = 10
    pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, pages-1))
    rows_page = rows[page*page_size:(page+1)*page_size]

    buttons = []
    for r in rows_page:
        buttons.append([InlineKeyboardButton(fmt_entity_row(r), callback_data=f"pick:{mode}:{r['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"page:{mode}:{page-1}"))
    if page < pages-1:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"page:{mode}:{page+1}"))
    if nav:
        buttons.append(nav)

    if mode == "add" and kind:
        buttons.append([InlineKeyboardButton("➕ Нет в реестре — создать", callback_data=CB_ADD_NEW_ENTITY)])

    caption = f"Найдено: {total}. Стр. {page+1}/{pages}"
    if isinstance(update_or_cb, Update):
        await update_or_cb.message.reply_text(caption, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update_or_cb.edit_message_text(caption, reply_markup=InlineKeyboardMarkup(buttons))

async def cb_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    _, mode, page_str = q.data.split(":")
    prefix = context.user_data.get("prefix", "")
    kind = context.user_data.get("kind")
    await send_entity_choices(q, mode, prefix, int(page_str), kind=kind)

async def cb_pick_entity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    _, mode, id_str = q.data.split(":")
    entity_id = int(id_str)
    context.user_data["entity_id"] = entity_id

    if mode == "add" or mode == "upd":
        await q.edit_message_text("Введите дату окончания подписи (напр. 31.12.2025).")
        context.user_data["awaiting"] = "expiry"
        context.user_data["flow"] = mode
    elif mode == "del":
        await show_and_confirm_delete(q, entity_id)
    elif mode == "regdel":
        await show_and_confirm_regdelete(q, entity_id)

async def cb_add_new_entity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await is_allowed(update.effective_user.id): return
    q = update.callback_query
    await q.answer()
    kind = context.user_data.get("kind")
    prefix = context.user_data.get("prefix", "")
    await q.edit_message_text(
        f"Создание новой записи в реестре ({'ЮЛ' if kind=='org' else 'ФЛ'}).\n"
        f"Введите *полное наименование* (сейчас в буфере «{safe_md(prefix)}»).",
        parse_mode=ParseMode.MARKDOWN
    )
    context.user_data["awaiting"] = "new_entity_name"

async def on_text_flow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает шаги ввода: имя новой сущности, дата, примечание."""
    if not await is_allowed(update.effective_user.id):
        return
    ud = context.user_data

    # Сначала получаем msg
    msg = update.message.text.strip() if update.message and update.message.text else ""

    # Глобальный "Назад" — всегда в главное меню
    if msg == BTN_BACK:
        await _go_main(context, update.effective_chat.id, silent=True, skip_next_on_text=True)
        return
    if ud.get("awaiting") == "note" and msg in MENU_BTNS:
        await update.message.reply_text(
            "Сначала введите примечание текстом или нажмите «Пропустить».",
        )
        return

    awaiting = ud.get("awaiting")
    if not awaiting:
        return  # обычный текст ловит on_text

    # --- Подтверждение автосоздания новой сущности при нуле совпадений ---
    if awaiting == "confirm_create":
        if msg == BTN_CREATE_CONFIRM:
            name = ud.get("proposed_name", "").strip()
            kind = ud.get("kind", "org")
            if not name:
                await update.message.reply_text("Название пустое. Попробуйте снова «Добавить подпись».")
                ud.clear()
                return
            async with aiosqlite.connect(DB_PATH) as db:
                try:
                    await db.execute("INSERT INTO entity(name, kind) VALUES (?,?)", (name, kind))
                    await db.commit()
                    async with db.execute("SELECT id FROM entity WHERE name=?", (name,)) as cur:
                        row = await cur.fetchone()
                        ud["entity_id"] = int(row[0])
                except aiosqlite.IntegrityError:
                    async with db.execute("SELECT id FROM entity WHERE name=?", (name,)) as cur:
                        row = await cur.fetchone()
                        ud["entity_id"] = int(row[0])
            ud["awaiting"] = "expiry"
            await update.message.reply_text(
                "Ок. Теперь введите дату окончания подписи (например 31.12.2025).",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True)
            )
            return
        elif msg in (BTN_CANCEL, BTN_BACK):
            ud.pop("awaiting", None)
            await update.message.reply_text(
                "Введите первые буквы/наименование ещё раз.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton(BTN_BACK)]], resize_keyboard=True)
            )
            return
        else:
            # ждём именно кнопку подтверждения/отмены
            return

    # --- Создание новой сущности в реестре ---
    if awaiting == "new_entity_name":
        name = msg
        kind = ud.get("kind", "org")
        async with aiosqlite.connect(DB_PATH) as db:
            try:
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
            await update.message.reply_text(
                f"✅ Добавлено в реестр: {ent_kind} {name}",
                reply_markup=main_menu_kbd(),
            )
            await _go_main(context, update.effective_chat.id, prompt=None, skip_next_on_text=True)
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
        await update_or_cb.message.reply_text(
            txt,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_kbd(),
        )
        await _go_main(context, update_or_cb.effective_chat.id, prompt=None, skip_next_on_text=True)
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
    if data == CB_ADD_KIND_ORG or data == CB_ADD_KIND_PERSON:
        await cb_add_kind(update, context); return
    if data == CB_ADD_NEW_ENTITY:
        await cb_add_new_entity(update, context); return
    if data.startswith("page:"):
        await cb_pagination(update, context); return
    if data.startswith("pick:"):
        await cb_pick_entity(update, context); return
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

async def send_reminders(application: Application):
    days_list = [25, 20, 15, 10, 5, 0]  # 0 = сегодня (можно убрать)
    today = date.today()
    targets = { (today + timedelta(days=d)).isoformat(): d for d in days_list }

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
        kind = "ЮЛ" if r["kind"]=="org" else "ФЛ"
        if diff > 0:
            header = f"⏰ Напоминание: через {diff} дн."
        elif diff == 0:
            header = "⚠️ Истекает сегодня!"
        else:
            header = f"❗ Просрочено на {-diff} дн."  # на всякий случай

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

async def _go_main(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    prompt: str | None = "Выберите действие кнопками ниже.",
    *,
    silent: bool = False,
    skip_next_on_text: bool = False,
):
    """Возвращает пользователя в главное меню и очищает состояние."""
    await context.bot.send_message(chat_id, "Главное меню", reply_markup=main_menu_kbd())
    context.user_data.clear()
    if skip_next_on_text:
        context.user_data["_skip_next_on_text"] = True

    if prompt is None or silent:
        return

    msg = await context.bot.send_message(chat_id, prompt, reply_markup=main_menu_kbd())


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





