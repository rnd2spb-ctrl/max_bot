#!/usr/bin/env python3
"""MAX-бот «картинка -> выжимка -> рассылка» на long polling (GET /updates). Версия 2.

README-ЗАМЕТКА (прочитайте перед запуском)
==========================================
Что делает бот. Бесконечный цикл опрашивает https://platform-api2.max.ru/updates,
каждое обновление обрабатывается в отдельной фоновой задаче:
    владелец -> картинка -> нейросеть (OpenRouter) -> архив -> рассылка получателям.
Данные (получатели, архив, настройки, marker опроса, входящая очередь) лежат в SQLite.

Запуск:
    pip install "httpx>=0.27,<1.0"        # на Windows ещё: pip install tzdata
    создайте файл .env рядом со скриптом (пример ниже) и выполните:
    python max_polling_bot.py

Файл .env (лежит рядом со скриптом; если его нет, берутся переменные окружения;
значения из окружения имеют приоритет над .env):
    MAX_BOT_TOKEN=...            # токен бота MAX (обязательно)
    OWNER_ID=123456              # user_id владельца в MAX (обязательно)
    OPENROUTER_API_KEY=...       # ключ OpenRouter (обязательно)
    OPENROUTER_MODEL=vendor/model-with-vision   # модель OpenRouter (обязательно)
    # необязательные: DB_PATH, SAVE_DIR, MAX_IMAGE_BYTES, ARCHIVE_DAYS, BOT_TZ,
    # MAX_UNSUBSCRIBE_WEBHOOK, LOG_LEVEL, LOG_DIR (папка журналов, по умолчанию logs рядом
    # со скриптом), LOG_KEEP_RUNS (сколько запусков хранить, по умолчанию 10),
    # OPENROUTER_REFERER, OPENROUTER_TITLE, POLL_TIMEOUT, POLL_LIMIT
    # MAX_BOT_NAME=имя_бота        # имя бота для персональных ссылок https://max.ru/<имя>?start=...
    # INVITE_TTL_HOURS=72          # срок действия персональной ссылки, часов (по умолчанию 72)
    # (значения по умолчанию см. раздел «Настройки» ниже)

ВАЖНО про модель. В образце limit_openrouter.py использовалась модель
nemotron-3.5-lightning:free. Для распознавания КАРТИНОК нужна модель, у которой в
architecture.input_modalities есть "image" (список: https://openrouter.ai/models,
фильтр по входным модальностям). Если модель текстовая, OpenRouter вернёт ошибку,
а бот ответит «Не удалось распознать изображение, пришлите ещё раз». При старте
бот сам сверяется с GET https://openrouter.ai/api/v1/models и пишет предупреждение,
если выбранная модель не заявляет поддержку изображений (проверка не блокирует запуск).

Формат запроса с картинкой (POST /api/v1/chat/completions): в messages[0].content
массив частей: сначала {"type":"text"}, затем {"type":"image_url","image_url":
{"url":"data:image/png;base64,..."}}. Статус: ПОДТВЕРЖДЕНО по руководству OpenRouter
«How to Send an Image to an LLM via API» (openrouter.ai/blog/tutorials/send-image-to-llm/,
16.11.2026): endpoint, массив частей с text первым, image_url.url как base64 data-URL,
типы PNG/JPEG/WebP/GIF. Оговорка: страница документации «Image Inputs» на 16.11.2026
описывает тот же сценарий через Responses API (/api/v1/responses, input_image); бот
использует /chat/completions, как в образце пользователя. Реальный запрос к
OpenRouter из этой среды не выполнялся.

Безопасность. Токен и ключ не пишутся в код и не выводятся в логи: любые значения
секретов и строки вида «Bearer …» маскируются в каждой строке лога. Ключ из файла
limit_openrouter.py в этот скрипт НЕ переносился: вписывайте свой ключ только в .env.
Если ключ побывал в чате или репозитории, отзовите его в OpenRouter и создайте новый.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import re
import secrets
import signal
import sqlite3
import struct
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Optional
from zoneinfo import ZoneInfo

import httpx

log = logging.getLogger("max_polling")

# =====================================================================
# 1. НАСТРОЙКИ: разбор .env, проверка обязательных переменных, маскировка секретов
# =====================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"

REQUIRED_VARS = ("MAX_BOT_TOKEN", "OWNER_ID", "OPENROUTER_API_KEY", "OPENROUTER_MODEL")
SECRET_VARS = ("MAX_BOT_TOKEN", "OPENROUTER_API_KEY")


def parse_env_text(text: str) -> dict[str, str]:
    """Разбор содержимого .env: KEY=VALUE, «export KEY=...», кавычки '...' и "...",
    комментарии «# ...» (в значении без кавычек комментарий начинается с « #»),
    BOM и CRLF. В двойных кавычках понимаются \\n, \\t, \\", \\\\."""
    out: dict[str, str] = {}
    for raw in text.lstrip("\ufeff").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, sep, val = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        val = val.strip()
        if val[:1] in ("'", '"'):
            q = val[0]
            end = -1
            i = 1
            while i < len(val):
                if q == '"' and val[i] == "\\":
                    i += 2
                    continue
                if val[i] == q:
                    end = i
                    break
                i += 1
            if end > 0:
                inner = val[1:end]
                if q == '"':
                    inner = re.sub(r"\\(.)", lambda m: {"n": "\n", "t": "\t"}.get(m.group(1), m.group(1)), inner)
                out[key] = inner
                continue
        m = re.search(r"\s#", val)                     # комментарий после значения
        out[key] = (val[:m.start()] if m else val).strip()
    return out


def load_env_file(path: Path = ENV_PATH, environ: Optional[dict] = None) -> list[str]:
    """Читает .env и дополняет им окружение. Уже заданные переменные окружения НЕ
    перезаписываются. Возвращает список переменных, взятых из файла. Нет файла: []."""
    env = os.environ if environ is None else environ
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as e:
        raise SystemExit(f"Не удалось прочитать {path}: {e}")
    loaded = []
    for k, v in parse_env_text(text).items():
        if env.get(k, "") == "":
            env[k] = v
            loaded.append(k)
    return loaded


class ConfigError(Exception):
    """Ошибка конфигурации: сообщение показывается пользователю как есть."""


@dataclass
class Config:
    bot_token: str = ""
    owner_id: int = 0
    openrouter_api_key: str = ""
    openrouter_model: str = ""
    db_path: str = str(SCRIPT_DIR / "max_bot.sqlite3")
    save_dir: Path = SCRIPT_DIR / "incoming_images"
    max_image_bytes: int = 10 * 1024 * 1024
    archive_days: int = 30
    tz: ZoneInfo = ZoneInfo("Europe/Moscow")
    unsubscribe_webhook: bool = False
    log_level: str = "INFO"
    openrouter_referer: str = "http://localhost"
    openrouter_title: str = "MAX Image Bot"
    poll_timeout: int = 30
    poll_limit: int = 100
    log_dir: Path = SCRIPT_DIR / "logs"
    log_keep_runs: int = 10
    bot_name: str = ""             # имя бота для ссылок https://max.ru/<имя>?start=...
    invite_ttl_hours: int = 72     # срок действия персональной ссылки


def _int(env: Mapping[str, str], name: str, default: int, lo: int, hi: int) -> int:
    raw = (env.get(name) or "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError:
        raise ConfigError(f"{name} должно быть целым числом, получено: {raw!r}")
    if not lo <= v <= hi:
        raise ConfigError(f"{name} должно быть в диапазоне {lo}..{hi}, получено: {v}")
    return v


def build_config(env: Mapping[str, str]) -> Config:
    """Собирает Config из словаря переменных. Бросает ConfigError с понятным текстом."""
    missing = [n for n in REQUIRED_VARS if not (env.get(n) or "").strip()]
    if missing:
        raise ConfigError(
            "Не заданы обязательные переменные: " + ", ".join(missing) + ".\n"
            f"Впишите их в файл {ENV_PATH} (строки вида ИМЯ=значение) или задайте в окружении.")
    owner_raw = env["OWNER_ID"].strip()
    if not re.fullmatch(r"-?\d+", owner_raw) or int(owner_raw) == 0:
        raise ConfigError(f"OWNER_ID должен быть числовым user_id владельца в MAX, получено: {owner_raw!r}")
    tz_name = (env.get("BOT_TZ") or "").strip() or "Europe/Moscow"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:                                    # ZoneInfoNotFoundError, ValueError
        raise ConfigError(f"BOT_TZ={tz_name!r}: часовой пояс не найден (на Windows: pip install tzdata)")
    level = ((env.get("LOG_LEVEL") or "").strip() or "INFO").upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        raise ConfigError(f"LOG_LEVEL={level!r}: допустимо DEBUG, INFO, WARNING, ERROR, CRITICAL")
    return Config(
        bot_token=env["MAX_BOT_TOKEN"].strip(),
        owner_id=int(owner_raw),
        openrouter_api_key=env["OPENROUTER_API_KEY"].strip(),
        openrouter_model=env["OPENROUTER_MODEL"].strip(),
        db_path=(env.get("DB_PATH") or "").strip() or str(SCRIPT_DIR / "max_bot.sqlite3"),
        save_dir=Path((env.get("SAVE_DIR") or "").strip() or SCRIPT_DIR / "incoming_images"),
        max_image_bytes=_int(env, "MAX_IMAGE_BYTES", 10 * 1024 * 1024, 1024, 1024 ** 3),
        archive_days=_int(env, "ARCHIVE_DAYS", 30, 1, 36500),
        tz=tz,
        unsubscribe_webhook=(env.get("MAX_UNSUBSCRIBE_WEBHOOK") or "0").strip().lower() in ("1", "true", "yes", "on"),
        log_level=level,
        openrouter_referer=(env.get("OPENROUTER_REFERER") or "").strip() or "http://localhost",
        openrouter_title=(env.get("OPENROUTER_TITLE") or "").strip() or "MAX Image Bot",
        poll_timeout=_int(env, "POLL_TIMEOUT", 30, 0, 90),
        poll_limit=_int(env, "POLL_LIMIT", 100, 1, 1000),
        log_dir=Path((env.get("LOG_DIR") or "").strip() or SCRIPT_DIR / "logs"),
        log_keep_runs=_int(env, "LOG_KEEP_RUNS", 10, 1, 1000),
        bot_name=(env.get("MAX_BOT_NAME") or "").strip().lstrip("@"),
        invite_ttl_hours=_int(env, "INVITE_TTL_HOURS", 72, 1, 720),
    )


# Текущая конфигурация. До load_config() стоят значения по умолчанию (секреты пустые),
# поэтому модуль безопасно импортируется (в том числе в тестах).
CFG = Config()


def load_config() -> Config:
    """Читает .env (если есть) + окружение и устанавливает глобальный CFG."""
    global CFG
    loaded = load_env_file()
    CFG = build_config(os.environ)
    log_file = configure_logging(CFG.log_level, CFG.log_dir, CFG.log_keep_runs)
    register_secrets(CFG.bot_token, CFG.openrouter_api_key)
    log.info("журнал: экран и файл %s (хранится запусков: %d)", log_file or "(файл не создан)", CFG.log_keep_runs)
    if not CFG.bot_name:
        log.warning("MAX_BOT_NAME не задан: ссылки-приглашения будут без адреса бота (впишите имя бота в .env)")
    log.info("настройки: .env %s; модель=%s; БД=%s; архив=%s дн.; пояс=%s; webhook-снятие=%s",
             f"найден (переменных из файла: {len(loaded)})" if ENV_PATH.exists() else "не найден, использую окружение",
             CFG.openrouter_model, CFG.db_path, CFG.archive_days, CFG.tz.key,
             "вкл" if CFG.unsubscribe_webhook else "выкл")
    log.info("секреты (замаскированы): MAX_BOT_TOKEN=%s OPENROUTER_API_KEY=%s",
             mask_secret(CFG.bot_token), mask_secret(CFG.openrouter_api_key))
    return CFG


# ---------- маскировка секретов в логах ----------
_SECRETS: set[str] = set()
_BEARER_RE = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=\-]{6,}")
_SKOR_RE = re.compile(r"sk-or-[A-Za-z0-9_\-]{6,}")


def mask_secret(value: str) -> str:
    """'abcdefghijkl' -> '***kl'. Короткие значения скрываются целиком."""
    if not value:
        return "(пусто)"
    return "***" + value[-2:] if len(value) >= 12 else "***"


def register_secrets(*values: str) -> None:
    _SECRETS.update(v for v in values if v and len(v) >= 4)


def redact(text: str) -> str:
    """Заменяет в тексте все зарегистрированные секреты, «Bearer …» и ключи вида sk-or-…"""
    for s in sorted(_SECRETS, key=len, reverse=True):
        if s in text:
            text = text.replace(s, mask_secret(s))
    text = _BEARER_RE.sub(r"\1***", text)
    return _SKOR_RE.sub("sk-or-***", text)


class RedactingFormatter(logging.Formatter):
    """Маскирует секреты во всей итоговой строке лога, включая трассировку исключения."""

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def prepare_log_file(log_dir: Path, keep_runs: int) -> Optional[Path]:
    """Путь файла журнала для этого запуска: bot_ГГГГММДД_ЧЧММСС.log.

    Старые файлы удаляются так, чтобы вместе с новым осталось не больше keep_runs
    (имя содержит дату, поэтому сортировка по имени = по времени). Нет доступа к
    папке: None, журнал будет только на экране."""
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(p for p in log_dir.glob("bot_*.log") if p.is_file())
        for old in existing[: max(0, len(existing) - (keep_runs - 1))]:
            try:
                old.unlink()
            except OSError:
                pass
        return log_dir / f"bot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    except OSError:
        return None


def configure_logging(level: str = "INFO", log_dir: Optional[Path] = None, keep_runs: int = 10) -> Optional[Path]:
    """Журнал на экран и (если задана папка) в файл этого запуска. Возвращает путь файла или None.
    Секреты маскируются в обоих местах."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_path = prepare_log_file(log_dir, keep_runs) if log_dir is not None else None
    if log_path is not None:
        try:
            handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
        except OSError:
            log_path = None
    for h in handlers:
        h.setFormatter(RedactingFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        root.addHandler(h)
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)   # httpx пишет полные URL запросов
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    if log_dir is not None and log_path is None:
        log.warning("файл журнала создать не удалось (%s): журнал только на экране", log_dir)
    return log_path


# =====================================================================
# 2. КОНСТАНТЫ И ТЕКСТЫ
# =====================================================================
POLL_TYPES = ["message_created", "bot_started"]
API = "https://platform-api2.max.ru"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODELS_URL = "https://openrouter.ai/api/v1/models"
AI_TIMEOUT = 60           # таймаут запроса к нейросети, секунды
AI_ATTEMPTS = 3           # всего попыток распознавания
AI_MAX_WAIT = 60.0        # дольше этого паузу не ждём (например, суточный лимит исчерпан)
MSG_LIMIT = 4000          # лимит длины одного сообщения MAX
BACKOFF_MAX = 60.0        # потолок паузы при ошибках опроса, секунды
SHUTDOWN_GRACE = 30.0     # сколько ждать фоновые задачи при остановке, секунды
MSK = ZoneInfo("Europe/Moscow")

# ---------- все тексты бота в одном месте ----------
MSG = {
    "not_invited": "Этот бот работает только по приглашению",
    "need_image": "Нужно прислать изображение",
    "bad_format": "Формат не поддерживается",
    "too_big": "Файл слишком большой",
    "no_file": "Не удалось получить файл, пришлите ещё раз",
    "ai_error": "Не удалось распознать изображение, пришлите ещё раз",
    "setup_required": "Настройка не завершена, изображения пока не обрабатываются.",
    "no_recipients": "Нет активных получателей.",
    "cancelled": "Отменено.",
    "not_started": "не доставлено (не запустил бота)",
    "send_failed": "не доставлено (ошибка отправки)",
}

DEFAULT_PROMPT = """Внимательно распознай текст с приложенного изображения, сохраняя исходную логику, и сделай из него сверхсжатую выжимку для сотрудника ПВЗ. Оставляй только то, что напрямую меняет его работу, влияет на деньги или устанавливает жесткие рамки.
Правила фильтрации (что ОСТАВЛЯТЬ):
Конкретные действия и их отмена: Что нужно делать (или больше не нужно делать) и при каком условии/статусе.
Жесткие рамки и KPI: Любые сроки (часы, даты периодов), проценты, деньги и лимиты (например, допустимое количество штрафов).
Структура: Если на изображении есть смысловые подзаголовки (разделы) — сохрани их. Если это сплошная новость без подзаголовков — не выдумывай их, дай суть 1–2 тезисами.
Правила очистки (что УДАЛЯТЬ безжалостно):
Предысторию и «воду»: Вводные фразы («Напомним, что...», «Также уточним...», «Ниже поделились...», описания того, как было раньше).
Неизменные процессы: Фразы о том, что остается по-старому или без изменений.
Детализацию и списки: Перечисления конкретных категорий товаров (списки с буллитами), очевидные технические шаги (упаковка в ящик, передача курьеру) и ссылки на разделы интерфейса («Проверить можно в разделе...»).
Формат вывода:
Первой строкой выведи краткий заголовок: суть сообщения одной строкой, до 10 слов, без точки в конце, без кавычек и без слова «Заголовок». Заголовок обязателен и не считается вступительной фразой.
Со следующей строки выведи основной текст: готовую выжимку по правилам выше.
Выводи только заголовок и выжимку, не печатай полный распознанный текст с картинки.
Формулируй максимально близко к тексту оригинала, без пересказа своими словами и без вступительных/заключительных фраз."""

_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep   # подменяется в тестах

# =====================================================================
# 3. БАЗА ДАННЫХ (SQLite)
# =====================================================================
_db: Optional[sqlite3.Connection] = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen (mid TEXT PRIMARY KEY, ts REAL);
CREATE TABLE IF NOT EXISTS notified (user_id INTEGER PRIMARY KEY, ts REAL);
CREATE TABLE IF NOT EXISTS started (user_id INTEGER PRIMARY KEY, ts REAL);
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS recipients (
    user_id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, title TEXT,
    text TEXT, image_path TEXT, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS deliveries (archive_id INTEGER, recipient_id INTEGER, status TEXT, ts REAL);
CREATE TABLE IF NOT EXISTS request_log (ts REAL, user_id INTEGER, text TEXT);
CREATE TABLE IF NOT EXISTS inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, payload TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
    created_ts REAL NOT NULL, expires_ts REAL NOT NULL,
    status TEXT NOT NULL,              -- ожидает | ждёт подтверждения | подключён | отозвана | истекла
    user_id INTEGER, used_ts REAL);
CREATE TABLE IF NOT EXISTS link_attempts (user_id INTEGER NOT NULL, ts REAL NOT NULL);
"""


def init_db(path: Optional[str] = None) -> None:
    """Открывает (создаёт) базу. Вызывается один раз при старте."""
    global _db
    if _db is not None:
        _db.close()
    _db = sqlite3.connect(path or CFG.db_path, check_same_thread=False)
    _db.executescript(SCHEMA)
    _db.commit()


def _conn() -> sqlite3.Connection:
    if _db is None:
        raise RuntimeError("база данных не открыта: вызовите init_db()")
    return _db


def db(sql: str, params: tuple = ()) -> sqlite3.Cursor:
    c = _conn()
    cur = c.execute(sql, params)
    c.commit()
    return cur


def q(sql: str, params: tuple = ()) -> list[tuple]:
    return _conn().execute(sql, params).fetchall()


def get_setting(key: str) -> Optional[str]:
    rows = q("SELECT value FROM settings WHERE key = ?", (key,))
    return rows[0][0] if rows else None


def set_setting(key: str, value: str) -> None:
    db("INSERT INTO settings (key, value) VALUES (?, ?) "
       "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))


def del_setting(key: str) -> None:
    db("DELETE FROM settings WHERE key = ?", (key,))


def load_marker() -> Optional[int]:
    v = get_setting("polling_marker")
    return int(v) if v else None


def save_marker(marker: int) -> None:
    set_setting("polling_marker", str(marker))


def get_prompt() -> str:
    return get_setting("prompt") or DEFAULT_PROMPT


def fmt_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, CFG.tz).strftime("%d.%m.%Y %H:%M")


def parse_date(s: str) -> Optional[tuple[float, float]]:
    for f in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            d = datetime.strptime(s.strip(), f).replace(tzinfo=CFG.tz)
            return d.timestamp(), d.timestamp() + 86400
        except ValueError:
            pass
    return None


# ---------- идемпотентность ----------
def claim_mid(mid: str) -> bool:
    """True, если сообщение с таким mid видим впервые (и помечаем его обработанным)."""
    return db("INSERT OR IGNORE INTO seen (mid, ts) VALUES (?, ?)", (mid, time.time())).rowcount == 1


def release_mid(mid: str) -> None:
    db("DELETE FROM seen WHERE mid = ?", (mid,))


def first_time_stranger(user_id: int) -> bool:
    """True, только если чужому ещё не отвечали «только по приглашению»."""
    return db("INSERT OR IGNORE INTO notified (user_id, ts) VALUES (?, ?)", (user_id, time.time())).rowcount == 1


def mark_started(user_id: int) -> None:
    db("INSERT OR REPLACE INTO started (user_id, ts) VALUES (?, ?)", (user_id, time.time()))


def has_started(user_id: int) -> bool:
    return bool(q("SELECT 1 FROM started WHERE user_id = ?", (user_id,)))


# ---------- персональные ссылки-приглашения ----------
# В базе хранится только SHA-256 от токена: сам токен виден владельцу один раз (при создании).
LINK_ATTEMPTS_PER_HOUR = 5
OPEN_INVITE = ("ожидает", "ждёт подтверждения")


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def create_invite(name: str) -> str:
    """Создаёт одноразовую ссылку для имени и возвращает токен."""
    token = secrets.token_urlsafe(16)                # 22 знака из A-Za-z0-9_-, с запасом до лимита 128
    now = time.time()
    db("INSERT INTO invites (name, token_hash, created_ts, expires_ts, status) VALUES (?, ?, ?, ?, 'ожидает')",
       (name, hash_token(token), now, now + CFG.invite_ttl_hours * 3600))
    return token


def invite_link(token: str) -> str:
    if CFG.bot_name:
        return f"https://max.ru/{CFG.bot_name}?start={token}"
    return f"start={token} (задайте MAX_BOT_NAME, тогда будет готовая ссылка)"


def invite_view(status: str, expires_ts: float, user_id: Optional[int]) -> str:
    """Статус приглашения для владельца (срок истечения вычисляется на лету)."""
    if status in OPEN_INVITE and expires_ts < time.time():
        return "истекла"
    if status == "ожидает":
        return f"ожидает, действует до {fmt_ts(expires_ts)}"
    if status == "ждёт подтверждения":
        return f"ждёт подтверждения имени (ID {user_id})"
    if status == "подключён":
        return f"подключён (ID {user_id})"
    return status                                     # отозвана, истекла


def name_busy(name: str) -> Optional[str]:
    """Причина, по которой имя занято, или None."""
    if q("SELECT 1 FROM recipients WHERE lower(name) = lower(?)", (name,)):
        return "уже подключён(а) как получатель"
    if q("SELECT 1 FROM invites WHERE lower(name) = lower(?) AND status IN ('ожидает', 'ждёт подтверждения')",
         (name,)):
        return "уже есть неиспользованная ссылка (чтобы заменить: /newlink имя)"
    return None


def parse_names(text: str) -> list[str]:
    """Имена из текста: каждое с новой строки (пустые строки пропускаются)."""
    return [n.strip() for n in text.splitlines() if n.strip()]


def make_links(names: list[str]) -> str:
    """Создаёт ссылки для имён и возвращает текст ответа владельцу."""
    lines = []
    for name in names:
        if len(name) > 40:
            lines.append(f"{name[:40]}…: слишком длинное имя (до 40 знаков)")
            continue
        busy = name_busy(name)
        if busy:
            lines.append(f"{name}: {busy}")
            continue
        lines.append(f"{name}: {invite_link(create_invite(name))}")
    return ("\n".join(lines) + f"\n\nКаждая ссылка одноразовая, действует {CFG.invite_ttl_hours} ч. "
            "Перешлите её получателю. Статусы: /links").strip()


# ---------- входящая очередь: защита от потери событий ----------
def inbox_store(updates: list[dict], marker: Optional[int]) -> list[int]:
    """Сохраняет обновления и новый marker ОДНОЙ транзакцией. Только после этого
    marker считается подтверждённым: при падении до обработки события не теряются,
    а будут обработаны при следующем старте (inbox_pending)."""
    c = _conn()
    ids: list[int] = []
    with c:                                              # транзакция: всё или ничего
        for u in updates:
            cur = c.execute("INSERT INTO inbox (ts, payload) VALUES (?, ?)",
                            (time.time(), json.dumps(u, ensure_ascii=False)))
            ids.append(int(cur.lastrowid))
        if marker is not None:
            c.execute("INSERT INTO settings (key, value) VALUES ('polling_marker', ?) "
                      "ON CONFLICT(key) DO UPDATE SET value = excluded.value", (str(marker),))
    return ids


def inbox_done(inbox_id: int) -> None:
    db("UPDATE inbox SET done = 1 WHERE id = ?", (inbox_id,))


def inbox_pending() -> list[tuple[int, dict]]:
    out = []
    for iid, payload in q("SELECT id, payload FROM inbox WHERE done = 0 ORDER BY id"):
        try:
            out.append((iid, json.loads(payload)))
        except ValueError:
            log.error("inbox id=%s: повреждённая запись, пропущена", iid)
            inbox_done(iid)
    return out


# =====================================================================
# 4. РАЗБИВКА ТЕКСТА И ОТПРАВКА СООБЩЕНИЙ MAX
# =====================================================================
def split_text(text: str, limit: int = MSG_LIMIT) -> list[str]:
    """Режет текст на части не длиннее limit по границам абзацев.

    Если частей больше одной, в начало каждой добавляется «(i/N)».
    Абзац, не влезающий в часть целиком, режется по строкам, затем по символам.
    """
    text = text.strip() or " "
    reserve = len("(99/99)\n")           # место под пометку
    cap = limit - reserve
    if len(text) <= limit:
        return [text]
    pieces: list[str] = []
    for para in text.split("\n\n"):
        while len(para) > cap:           # слишком длинный абзац
            cut = para.rfind("\n", 0, cap)
            if cut <= 0:
                cut = para.rfind(" ", 0, cap)
            if cut <= 0:
                cut = cap
            pieces.append(para[:cut].rstrip())
            para = para[cut:].lstrip()
        if para:
            pieces.append(para)
    parts: list[str] = []
    cur = ""
    for p in pieces:
        cand = p if not cur else cur + "\n\n" + p
        if len(cand) <= cap:
            cur = cand
        else:
            parts.append(cur)
            cur = p
    if cur:
        parts.append(cur)
    n = len(parts)
    if n == 1:
        return parts
    return [f"({i}/{n})\n{p}" for i, p in enumerate(parts, 1)]


# ---------- очередь отправки: не более 2 сообщений в секунду в один чат ----------
_last_send: dict[int, float] = {}
_send_locks: dict[int, asyncio.Lock] = {}
SEND_ATTEMPTS = 5


def retry_after_seconds(headers: Mapping[str, str], default: float, cap: float = 30.0) -> float:
    """Retry-After в секундах или как HTTP-дата; результат ограничен [0.5, cap]."""
    raw = headers.get("Retry-After")
    if raw is None:
        return default
    sec = parse_retry_after(raw)
    return default if sec is None else min(cap, max(0.5, sec))


def parse_retry_after(raw: str) -> Optional[float]:
    raw = raw.strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, dt.timestamp() - time.time())
    except (TypeError, ValueError):
        return None


async def send_message(client: httpx.AsyncClient, user_id: int, text: str) -> str:
    """Одно сообщение. Возвращает «ok», «rejected» (4xx, кроме 429: чаще всего
    пользователь не нажал Start) или «failed» (сеть/429/5xx после повторов)."""
    lock = _send_locks.setdefault(user_id, asyncio.Lock())
    async with lock:
        for attempt in range(SEND_ATTEMPTS):
            wait = 0.5 - (time.monotonic() - _last_send.get(user_id, 0.0))
            if wait > 0:
                await _sleep(wait)
            pause = float(2 ** attempt)
            try:
                r = await client.post(
                    f"{API}/messages",
                    params={"user_id": user_id},
                    headers={"Authorization": CFG.bot_token},
                    json={"text": text[:MSG_LIMIT]},
                    timeout=15,
                )
            except httpx.HTTPError as e:
                log.warning("отправка user_id=%s: сетевая ошибка %r", user_id, e)
            else:
                _last_send[user_id] = time.monotonic()
                if r.status_code == 200:
                    return "ok"
                if r.status_code == 429:                       # превышен лимит отправки
                    pause = retry_after_seconds(r.headers, pause)
                    log.warning("отправка user_id=%s: 429, повтор через %.1f с", user_id, pause)
                elif r.status_code >= 500:
                    log.warning("отправка user_id=%s: HTTP %s, повтор через %.1f с", user_id, r.status_code, pause)
                else:
                    log.error("отправка user_id=%s отклонена: HTTP %s %s", user_id, r.status_code, r.text[:200])
                    return "rejected"
            await _sleep(pause)
        log.error("отправка user_id=%s: исчерпаны попытки (%d)", user_id, SEND_ATTEMPTS)
        return "failed"


async def send_text(client: httpx.AsyncClient, user_id: int, text: str) -> str:
    """Длинный текст уходит частями до 4000 символов с пометками «(1/N)». При сбое части — стоп."""
    for part in split_text(text):
        status = await send_message(client, user_id, part)
        if status != "ok":
            return status
    return "ok"


# ---------- служебные сообщения владельцу ----------
_http: Optional[httpx.AsyncClient] = None       # общий клиент, задаётся в amain()
_ALERT_LAST: dict[str, float] = {}
_ALERT_SENT: list[float] = []
ALERT_PER_HOUR = 20


async def alert_owner(text: str, key: Optional[str] = None, every: float = 900.0, force: bool = False) -> None:
    """Служебное сообщение владельцу (ошибка, предупреждение, итог) и запись в журнал.

    key + every: сообщение с тем же key не чаще раза в every секунд. Всего не более
    ALERT_PER_HOUR в час (защита от лавины). force=True обходит оба ограничения.
    Сбой самой отправки только пишется в журнал, поэтому рекурсии нет."""
    text = redact(text)
    now = time.time()
    if not force:
        if key and now - _ALERT_LAST.get(key, 0.0) < every:
            log.debug("служебное сообщение пропущено (повтор %s): %s", key, text)
            return
        _ALERT_SENT[:] = [t for t in _ALERT_SENT if now - t < 3600]
        if len(_ALERT_SENT) >= ALERT_PER_HOUR:
            log.warning("служебное сообщение владельцу пропущено: лимит %d в час. Текст: %s",
                        ALERT_PER_HOUR, text.replace("\n", " | "))
            return
    if key:
        _ALERT_LAST[key] = now
    _ALERT_SENT.append(now)
    log.info("служебное сообщение владельцу: %s", text.replace("\n", " | "))
    if _http is None or not CFG.owner_id:
        return
    try:
        status = await send_text(_http, CFG.owner_id, text)
        if status != "ok":
            log.warning("служебное сообщение владельцу не доставлено (%s)", status)
    except Exception:
        log.exception("служебное сообщение владельцу: сбой отправки")


# =====================================================================
# 5. РАЗБОР СОБЫТИЯ И СКАЧИВАНИЕ ИЗОБРАЖЕНИЯ
# =====================================================================
def display_name(user: Any) -> str:
    """Имя пользователя из события для сообщений владельцу: одна строка, до 60 знаков.
    Имя пишет сам пользователь, поэтому ему нельзя доверять (только показываем)."""
    if not isinstance(user, dict):
        return ""
    name = (user.get("name")
            or " ".join(str(p) for p in (user.get("first_name"), user.get("last_name")) if p)
            or user.get("username") or "")
    return " ".join(str(name).split())[:60]


def parse_message(update: dict) -> Optional[dict]:
    if update.get("update_type") != "message_created":
        return None
    msg = update.get("message") or {}
    body = msg.get("body") or {}
    sender = msg.get("sender") or {}
    if sender.get("is_bot"):
        return None
    atts = body.get("attachments") or []
    return {
        "mid": body.get("mid"),
        "sender_id": sender.get("user_id"),
        "sender_name": display_name(sender),
        "text": (body.get("text") or "").strip(),
        "images": [a.get("payload") or {} for a in atts
                   if a.get("type") == "image" and (a.get("payload") or {}).get("url")],
    }


PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def sniff_format(head: bytes) -> Optional[str]:
    """Определяет формат по первым байтам: «jpg», «png», «webp» или None.

    PNG проверяется строже прежней версии: 8-байтовая сигнатура, затем обязательный
    первый чанк IHDR (длина 13, тип IHDR) и ненулевые ширина/высота. Нужно ≥ 24 байт."""
    if head.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if head.startswith(PNG_SIGNATURE):
        if len(head) < 24 or head[8:12] != b"\x00\x00\x00\r" or head[12:16] != b"IHDR":
            return None
        width, height = struct.unpack(">II", head[16:24])
        return "png" if width > 0 and height > 0 else None
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "webp"
    return None


class ImageError(Exception):
    """Картинку нельзя принять; текст сообщения можно показать владельцу."""


async def download_image(client: httpx.AsyncClient, url: str) -> tuple[bytes, str]:
    last: Optional[Exception] = None
    for attempt in (1, 2):
        try:
            buf = bytearray()
            async with client.stream("GET", url, timeout=30, follow_redirects=True) as r:
                r.raise_for_status()
                async for chunk in r.aiter_bytes():
                    buf += chunk
                    if len(buf) > CFG.max_image_bytes:
                        raise ImageError(MSG["too_big"])
            fmt = sniff_format(bytes(buf[:32]))
            if fmt is None:
                raise ImageError(MSG["bad_format"])
            return bytes(buf), fmt
        except ImageError:
            raise
        except (httpx.HTTPError, OSError) as e:
            last = e
            log.warning("скачивание изображения, попытка %d: %r", attempt, e)
            if attempt == 1:
                await _sleep(5)
    raise RuntimeError(f"Не удалось скачать: {last}")


# =====================================================================
# 6. КЛИЕНТ НЕЙРОСЕТИ (OpenRouter) И ЛИМИТЫ
# =====================================================================
class AIError(Exception):
    """Распознавание не удалось (после повторов или без смысла повторять).

    kind: limit (лимит исчерпан), credits (нет кредитов), auth (ключ или доступ),
    model (модель не найдена), other."""

    def __init__(self, msg: str, kind: str = "other"):
        super().__init__(msg)
        self.kind = kind


@dataclass
class RateLimitInfo:
    limit: Optional[int] = None
    remaining: Optional[int] = None
    reset_ts: Optional[float] = None        # unix-время сброса, секунды
    retry_after: Optional[float] = None     # секунд до повтора


_last_rate: Optional[RateLimitInfo] = None


def parse_reset(value: Any) -> Optional[float]:
    """X-RateLimit-Reset: миллисекунды или секунды unix-времени.
    Если значение больше 1_000_000_000_000, это миллисекунды (делим на 1000)."""
    try:
        v = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v / 1000.0 if v > 1_000_000_000_000 else v


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


def parse_rate_limits(headers: Mapping[str, str], body: Any = None) -> RateLimitInfo:
    """Лимиты из заголовков ответа (приоритет) и из error.metadata.headers тела ответа.
    Имена заголовков регистронезависимы."""
    merged: dict[str, str] = {}
    meta = ((body.get("error") or {}).get("metadata") or {}).get("headers") if isinstance(body, dict) \
        and isinstance(body.get("error"), dict) else None
    if isinstance(meta, dict):
        merged.update({str(k).lower(): str(v) for k, v in meta.items()})
    merged.update({k.lower(): v for k, v in headers.items()})    # заголовки главнее
    info = RateLimitInfo(
        limit=_to_int(merged.get("x-ratelimit-limit")),
        remaining=_to_int(merged.get("x-ratelimit-remaining")),
        reset_ts=parse_reset(merged["x-ratelimit-reset"]) if "x-ratelimit-reset" in merged else None,
    )
    if "retry-after" in merged:
        info.retry_after = parse_retry_after(merged["retry-after"])
    return info


def format_duration(seconds: float) -> str:
    s = max(0, int(seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h} ч {m:02d} мин"
    if m:
        return f"{m} мин {sec:02d} с"
    return f"{sec} с"


def format_reset(reset_ts: float, now: Optional[float] = None) -> str:
    """«сброс 17.11.2026 00:00:00 UTC / 17.11.2026 03:00:00 МСК, осталось 8 ч 30 мин»."""
    now = time.time() if now is None else now
    utc = datetime.fromtimestamp(reset_ts, timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")
    msk = datetime.fromtimestamp(reset_ts, MSK).strftime("%d.%m.%Y %H:%M:%S МСК")
    left = reset_ts - now
    tail = f"осталось {format_duration(left)}" if left > 0 else "срок сброса уже наступил"
    return f"сброс {utc} / {msk}, {tail}"


def describe_limits(info: RateLimitInfo) -> str:
    parts = []
    if info.limit is not None or info.remaining is not None:
        parts.append(f"осталось запросов: {info.remaining if info.remaining is not None else '?'}"
                     f" из {info.limit if info.limit is not None else '?'}")
    if info.reset_ts is not None:
        parts.append(format_reset(info.reset_ts))
    if info.retry_after is not None:
        parts.append(f"Retry-After {info.retry_after:.0f} с")
    return "; ".join(parts) or "заголовков лимитов нет"


def build_ai_payload(prompt: str, data: bytes, fmt: str) -> dict:
    """Тело запроса /chat/completions: текст первым, затем картинка как data-URL (base64)."""
    mime = {"jpg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[fmt]
    b64 = base64.b64encode(data).decode()
    return {
        "model": CFG.openrouter_model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ],
        }],
    }


def ai_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {CFG.openrouter_api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": CFG.openrouter_referer,
        "X-Title": CFG.openrouter_title,
    }


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except ValueError:
        return None


def _error_text(body: Any, r: httpx.Response) -> str:
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        e = body["error"]
        return f"{e.get('code', '')} {e.get('message', '')}".strip()[:300]
    return (r.text or "")[:300]


async def recognize(client: httpx.AsyncClient, data: bytes, fmt: str) -> str:
    """Отправляет картинку в OpenRouter и возвращает текст ответа.

    Повторы (всего AI_ATTEMPTS): HTTP 408/429/5xx, сеть, таймаут, пустой ответ.
    При 429 пауза берётся из Retry-After, иначе из X-RateLimit-Reset, иначе 5·2^n с;
    если ждать нужно дольше AI_MAX_WAIT (лимит исчерпан до завтра), повтор не делается.
    Остальные 4xx (400/401/402/403) повторять бессмысленно. Итог ошибки: AIError."""
    global _last_rate
    if not CFG.openrouter_api_key or not CFG.openrouter_model:
        raise AIError("OpenRouter не настроен (OPENROUTER_API_KEY / OPENROUTER_MODEL)")
    payload = build_ai_payload(get_prompt(), data, fmt)
    last_err = "нет ответа"
    last_kind = "other"
    for attempt in range(1, AI_ATTEMPTS + 1):
        pause = 5.0 * 2 ** (attempt - 1)
        last_kind = "other"
        try:
            r = await client.post(OPENROUTER_URL, headers=ai_headers(), json=payload, timeout=AI_TIMEOUT)
        except httpx.HTTPError as e:
            last_err = f"сеть/таймаут: {e!r}"
            log.warning("OpenRouter, попытка %d/%d: %s", attempt, AI_ATTEMPTS, last_err)
        else:
            body = _safe_json(r)
            info = parse_rate_limits(r.headers, body)
            if info.limit is not None or info.remaining is not None or info.reset_ts is not None:
                _last_rate = info
                log.info("лимиты OpenRouter: %s", describe_limits(info))
                if info.remaining is not None and info.remaining <= 0:
                    await alert_owner("Предупреждение: у модели закончились лимиты запросов OpenRouter ("
                                      + describe_limits(info) + "). Изображения могут не обрабатываться.",
                                      key="limit_zero", every=3600.0)
            err_in_body = isinstance(body, dict) and isinstance(body.get("error"), dict) and not body.get("choices")
            status = r.status_code
            if err_in_body and status == 200:                      # ошибка внутри HTTP 200
                status = _to_int(body["error"].get("code")) or 502
            if status == 200:
                try:
                    text = (body["choices"][0]["message"]["content"] or "").strip()
                except (KeyError, IndexError, TypeError):
                    text = ""
                if text:
                    return text
                last_err = "пустой или неожиданный ответ нейросети"
                log.warning("OpenRouter, попытка %d/%d: %s", attempt, AI_ATTEMPTS, last_err)
            elif status == 429 or status == 408 or status >= 500:
                last_err = f"HTTP {status}: {_error_text(body, r)}"
                last_kind = "limit" if status == 429 else "other"
                if status == 429:
                    _last_rate = info
                    wait = info.retry_after
                    if wait is None and info.reset_ts is not None:
                        wait = info.reset_ts - time.time() + 1.0
                    wait = pause if wait is None else max(1.0, wait)
                    log.warning("OpenRouter 429 (%s): %s", describe_limits(info), last_err)
                    if wait > AI_MAX_WAIT:
                        raise AIError(f"лимит исчерпан, ждать {format_duration(wait)} (> {AI_MAX_WAIT:.0f} с): {last_err}",
                                      kind="limit")
                    pause = wait
                else:
                    log.warning("OpenRouter, попытка %d/%d: %s", attempt, AI_ATTEMPTS, last_err)
            else:
                hint = {401: " (проверьте OPENROUTER_API_KEY)", 402: " (недостаточно кредитов)",
                        403: " (доступ запрещён или модерация)", 404: " (проверьте OPENROUTER_MODEL)"}.get(status, "")
                kind = {401: "auth", 402: "credits", 403: "auth", 404: "model"}.get(status, "other")
                raise AIError(f"HTTP {status}{hint}: {_error_text(body, r)}", kind=kind)
        if attempt < AI_ATTEMPTS:
            log.info("OpenRouter: пауза %.1f с перед повтором", pause)
            await _sleep(pause)
    raise AIError(f"после {AI_ATTEMPTS} попыток: {last_err}", kind=last_kind)


async def check_model_vision(client: httpx.AsyncClient) -> None:
    """Проверка при старте по каталогу OpenRouter.

    Модель без входа «image»: владельцу уходит сообщение и бот останавливается (SystemExit).
    Модели нет в каталоге: предупреждение в журнал и владельцу, запуск продолжается
    (бывают псевдонимы вроде openrouter/auto). Каталог недоступен: проверка пропускается."""
    try:
        r = await client.get(OPENROUTER_MODELS_URL, timeout=15)
        if r.status_code != 200:
            log.info("проверка модели пропущена: HTTP %s", r.status_code)
            return
        catalog = r.json().get("data") or []
    except (httpx.HTTPError, ValueError, AttributeError) as e:
        log.info("проверка модели пропущена: %r", e)
        return
    for m in catalog:
        if not isinstance(m, dict) or m.get("id") != CFG.openrouter_model:
            continue
        mods = (m.get("architecture") or {}).get("input_modalities") or []
        if "image" in mods:
            log.info("модель %s заявляет поддержку изображений", CFG.openrouter_model)
            return
        msg = (f"Бот остановлен: модель {CFG.openrouter_model} не принимает изображения "
               f"(input_modalities={mods}). Укажите в OPENROUTER_MODEL модель с поддержкой image: "
               "https://openrouter.ai/models")
        await alert_owner(msg, force=True)
        raise SystemExit(msg)
    await alert_owner(f"Предупреждение: модель {CFG.openrouter_model} не найдена в каталоге OpenRouter. "
                      "Проверьте OPENROUTER_MODEL; если это псевдоним, можно игнорировать.",
                      key="model_not_found", every=3600.0)


# =====================================================================
# 7. ПОЛУЧАТЕЛИ, АРХИВ, РАССЫЛКА
# =====================================================================
def active_recipients() -> list[tuple[int, str]]:
    return q("SELECT user_id, name FROM recipients WHERE active = 1 ORDER BY name")


def is_recipient(user_id: int) -> bool:
    return bool(q("SELECT 1 FROM recipients WHERE user_id = ?", (user_id,)))


def find_recipient(text: str) -> Optional[tuple[int, str]]:
    text = text.strip()
    if text.isdigit():
        rows = q("SELECT user_id, name FROM recipients WHERE user_id = ?", (int(text),))
    else:
        rows = q("SELECT user_id, name FROM recipients WHERE lower(name) = lower(?)", (text,))
    return rows[0] if rows else None


def parse_add(arg: str) -> Optional[tuple[str, int]]:
    m = re.fullmatch(r"(.+?)\s+(\d+)", arg.strip())
    return (m.group(1).strip(), int(m.group(2))) if m else None


def split_title_body(result: str) -> tuple[str, str]:
    """Ответ нейросети -> (заголовок, тело). Заголовок: первая непустая строка, тело: всё остальное.
    Слишком длинную первую строку (больше 200 знаков) режем по пробелу, остаток идёт в начало тела."""
    lines = result.strip().splitlines()
    for i, ln in enumerate(lines):
        if not ln.strip():
            continue
        first, rest = ln.strip(), lines[i + 1:]
        if len(first) > 200:
            cut = first.rfind(" ", 0, 200)
            cut = cut if cut > 100 else 200
            first, rest = first[:cut].rstrip(), [ln.strip()[cut:].lstrip()] + rest
        return first, "\n".join(rest).strip()
    return "", ""


def compose_text(title: str, body: str) -> str:
    """Текст для рассылки и повторов: заголовок, пустая строка, тело (если оно есть)."""
    return f"{title}\n\n{body}" if body else title


def archive_add(ts: float, title: str, text: str, image_path: str, status: str) -> int:
    return int(db("INSERT INTO archive (ts, title, text, image_path, status) VALUES (?, ?, ?, ?, ?)",
                  (ts, title, text, image_path, status)).lastrowid)


def archive_rows(recipient_id: Optional[int] = None) -> list[tuple]:
    """Записи от новых к старым. Для получателя: только адресованные ему и доставленные."""
    if recipient_id is None:
        return q("SELECT id, ts, title, text, status FROM archive ORDER BY id DESC")
    return q("SELECT id, ts, title, text, status FROM archive WHERE status = 'успех' AND id IN "
             "(SELECT archive_id FROM deliveries WHERE recipient_id = ? AND status = 'ok') ORDER BY id DESC",
             (recipient_id,))


def filter_rows(rows: list[tuple], query: str) -> list[tuple]:
    """Поиск по дате (ДД.ММ.ГГГГ или ГГГГ-ММ-ДД) либо по слову в заголовке и тексте."""
    query = query.strip()
    d = parse_date(query)
    if d:
        return [r for r in rows if d[0] <= r[1] < d[1]]
    low = query.lower()
    return [r for r in rows if low in (r[2] or "").lower() or low in (r[3] or "").lower()]


def purge_archive() -> None:
    """Архив хранится ARCHIVE_DAYS дней: удаляем записи и файлы старше срока.
    Заодно чистим выполненные записи входящей очереди и старые mid."""
    now = time.time()
    cutoff = now - CFG.archive_days * 86400
    for aid, path in q("SELECT id, image_path FROM archive WHERE ts < ?", (cutoff,)):
        if path:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError as e:
                log.warning("не удалось удалить файл %s: %r", path, e)
        db("DELETE FROM deliveries WHERE archive_id = ?", (aid,))
        db("DELETE FROM archive WHERE id = ?", (aid,))
    db("DELETE FROM inbox WHERE done = 1 AND ts < ?", (now - 86400,))
    db("DELETE FROM seen WHERE ts < ?", (cutoff,))


async def deliver(client: httpx.AsyncClient, archive_id: int, ts: float, text: str,
                  recs: list[tuple[int, str]]) -> tuple[int, list[str]]:
    """Отправляет результат получателям. Сбой одного не останавливает остальных.

    Второй элемент результата — строки «Имя — причина» по недоставленным.
    """
    body = f"Результат от {fmt_ts(ts)}:\n{text}"
    ok, failed = 0, []
    for uid, name in recs:
        status = await send_text(client, uid, body)
        db("INSERT INTO deliveries (archive_id, recipient_id, status, ts) VALUES (?, ?, ?, ?)",
           (archive_id, uid, "ok" if status == "ok" else "fail", time.time()))
        if status == "ok":
            ok += 1
        elif status == "rejected" and not has_started(uid):
            failed.append(name)
            log.warning("не доставлено %s (%s): %s", name, uid, MSG["not_started"])
        else:
            failed.append(name)
            log.warning("не доставлено %s (%s): %s", name, uid, MSG["send_failed"])
    return ok, failed


def delivery_summary(ok: int, total: int, failed: list[str]) -> str:
    """«Отправлено: N из M. Не доставлено: [имена].»"""
    s = f"Отправлено: {ok} из {total}."
    if failed:
        s += " Не доставлено: " + ", ".join(failed) + "."
    paused = q("SELECT COUNT(*) FROM recipients WHERE active = 0")[0][0]
    if paused:
        s += f" Отключено (рассылку не получают): {paused}."
    return s


# ---------- запросы получателей ----------
async def handle_recipient_request(client: httpx.AsyncClient, user_id: int, text: str) -> None:
    db("INSERT INTO request_log (ts, user_id, text) VALUES (?, ?, ?)", (time.time(), user_id, text))
    log.info("запрос получателя user_id=%s: %s", user_id, text[:80])
    t = text.strip().lower()
    help_text = ("Доступно: «повтори последнее», «повтори [номер]», "
                 "«повтори по теме [слово]», «повтори за [дата ДД.ММ.ГГГГ]».")
    if not t.startswith("повтори"):
        await send_text(client, user_id, help_text)
        return
    rest = t[len("повтори"):].strip()
    rows = archive_rows(user_id)
    chosen: list[tuple] = []
    if rest in ("", "последнее"):
        chosen = rows[:1]
    elif rest.isdigit():
        chosen = [r for r in rows if r[0] == int(rest)]
    elif rest.startswith("по теме "):
        chosen = filter_rows(rows, rest[len("по теме "):])
    elif rest.startswith("за "):
        chosen = filter_rows(rows, rest[3:]) if parse_date(rest[3:]) else []
    else:
        await send_text(client, user_id, help_text)
        return
    if not chosen:
        await send_text(client, user_id, "Ничего не найдено.")
    elif len(chosen) == 1:
        r = chosen[0]
        await send_text(client, user_id, f"Результат от {fmt_ts(r[1])}:\n{compose_text(r[2], r[3])}")
    else:
        lines = [f"{r[0]}. {fmt_ts(r[1])} — {r[2]}" for r in chosen[:5]]
        await send_text(client, user_id, "Найдено:\n" + "\n".join(lines) +
                        "\nНапишите «повтори [номер]».")


# =====================================================================
# 8. КОМАНДЫ ВЛАДЕЛЬЦА
# =====================================================================
async def handle_invite_start(client: httpx.AsyncClient, uid: int, token: str, name: str) -> None:
    """Пользователь открыл персональную ссылку (bot_started с payload)."""
    now = time.time()
    db("DELETE FROM link_attempts WHERE ts < ?", (now - 86400,))
    db("INSERT INTO link_attempts (user_id, ts) VALUES (?, ?)", (uid, now))
    tries = q("SELECT COUNT(*) FROM link_attempts WHERE user_id = ? AND ts > ?", (uid, now - 3600))[0][0]
    if tries > LINK_ATTEMPTS_PER_HOUR:
        log.warning("слишком много попыток по ссылке: user_id=%s", uid)
        await alert_owner(f"ID {uid} ({name or 'без имени'}) слишком часто открывает ссылку-приглашение. "
                          "Ответ ему временно не отправляется.", key=f"linkrl:{uid}", every=3600.0)
        return
    if is_recipient(uid):
        await send_text(client, uid, "Вы уже подключены и получаете результаты.")
        return
    row = q("SELECT id, name, status, expires_ts, user_id FROM invites WHERE token_hash = ?", (hash_token(token),))
    inv = row[0] if row else None
    usable = inv is not None and (inv[2] == "ожидает" or (inv[2] == "ждёт подтверждения" and inv[4] == uid))
    if not usable:
        log.info("неизвестная или занятая ссылка от user_id=%s", uid)
        await send_text(client, uid, "Ссылка недействительна, уже использована или истекла. "
                                     "Попросите отправителя прислать новую.")
        await alert_owner(f"По ссылке-приглашению пришёл неверный или уже использованный токен: "
                          f"ID {uid} ({name or 'без имени'}).", key=f"badlink:{uid}", every=3600.0)
        return
    inv_id, inv_name, status, expires, _ = inv
    if expires < now:
        db("UPDATE invites SET status = 'истекла' WHERE id = ?", (inv_id,))
        await send_text(client, uid, "Срок ссылки истёк. Попросите отправителя прислать новую.")
        return
    db("UPDATE invites SET status = 'ждёт подтверждения', user_id = ? WHERE id = ?", (uid, inv_id))
    log.info("ссылка открыта: приглашение #%s, user_id=%s", inv_id, uid)
    await send_text(client, uid, f"Здравствуйте! Вас приглашают получать результаты работы бота. "
                                 f"Вы — {inv_name}? Ответьте «да», чтобы подтвердить.")


async def handle_invite_reply(client: httpx.AsyncClient, uid: int, text: str) -> bool:
    """Ответ пользователя на вопрос «Вы — <имя>?». True, если сообщение относится к приглашению."""
    row = q("SELECT id, name, expires_ts FROM invites WHERE user_id = ? AND status = 'ждёт подтверждения'", (uid,))
    if not row:
        return False
    inv_id, name, expires = row[0]
    now = time.time()
    if expires < now:
        db("UPDATE invites SET status = 'истекла' WHERE id = ?", (inv_id,))
        await send_text(client, uid, "Срок ссылки истёк. Попросите отправителя прислать новую.")
        return True
    if text.strip().lower() != "да":
        await send_text(client, uid, f"Чтобы подтвердить, что вы — {name}, ответьте «да».")
        return True
    db("UPDATE invites SET status = 'подключён', used_ts = ? WHERE id = ?", (now, inv_id))
    db("INSERT OR IGNORE INTO recipients (user_id, name, active) VALUES (?, ?, 1)", (uid, name))
    mark_started(uid)
    log.info("получатель подключён по приглашению #%s: user_id=%s", inv_id, uid)
    await send_text(client, uid, "Готово, вы подключены: будете получать результаты.")
    await alert_owner(f"{name} (ID {uid}) подключился(ась) по ссылке.", key=f"linked:{inv_id}", every=60.0)
    return True


HELP = """/recipients — список получателей
/add [имя] — создать персональную ссылку (можно несколько имён, каждое с новой строки)
/links — статусы ссылок
/newlink [имя] — заменить неиспользованную ссылку новой
/revoke [имя] — отозвать неиспользованную ссылку
/remove [имя или ID] — удалить получателя
/pause [имя или ID] — временно не слать получателю
/resume [имя или ID] — снова слать получателю
/archive [N] — последние N записей (по умолчанию 10)
/archive find [слово или дата] — поиск в архиве
/resend [номер] [получатель или все] — повторная отправка
/prompt — показать промпт
/prompt set [текст] — заменить промпт
/status — состояние
/help — этот список"""


async def handle_owner_command(client: httpx.AsyncClient, owner_id: int, text: str) -> None:
    parts = text.strip().split(None, 1)              # перенос строки тоже разделяет команду и аргумент
    cmd = parts[0].lower() if parts else ""
    arg = parts[1].strip() if len(parts) > 1 else ""
    log.info("команда владельца: %s", cmd)

    async def reply(s: str) -> None:
        await send_text(client, owner_id, s)

    if cmd == "/help":
        await reply(HELP)
    elif cmd == "/recipients":
        rows = q("SELECT user_id, name, active FROM recipients ORDER BY name")
        await reply("\n".join(f"{n} — {u} — {'активен' if a else 'отключён'}" for u, n, a in rows)
                    or "Получателей нет.")
    elif cmd == "/add":
        names = parse_names(arg)
        if not names:
            await reply("Формат: /add Имя. Несколько имён — каждое с новой строки.")
        else:
            await reply(make_links(names))
    elif cmd == "/links":
        rows = q("SELECT name, status, expires_ts, user_id FROM invites ORDER BY id DESC LIMIT 50")
        await reply("\n".join(f"{n} — {invite_view(s, e, u)}" for n, s, e, u in rows)
                    or "Ссылок ещё не создавали. Создать: /add Имя.")
    elif cmd == "/newlink":
        if not arg:
            await reply("Формат: /newlink Имя")
        elif find_recipient(arg):
            await reply(f"{arg} уже подключён(а). Новая ссылка нужна до подключения; чтобы заменить "
                        "получателя, сначала /remove.")
        else:
            n = db("UPDATE invites SET status = 'отозвана' WHERE lower(name) = lower(?) "
                   "AND status IN ('ожидает', 'ждёт подтверждения')", (arg,)).rowcount
            log.info("перевыпуск ссылки: %s, отозвано старых: %s", arg, n)
            await reply(make_links([arg]))
    elif cmd == "/revoke":
        n = db("UPDATE invites SET status = 'отозвана' WHERE lower(name) = lower(?) "
               "AND status IN ('ожидает', 'ждёт подтверждения')", (arg,)).rowcount
        if n:
            await reply(f"Ссылка для «{arg}» отозвана. Старая ссылка больше не работает.")
        elif find_recipient(arg):
            await reply(f"{arg} уже подключён(а): чтобы убрать из рассылки, используйте /remove.")
        else:
            await reply(f"Активной ссылки для «{arg}» нет.")
    elif cmd == "/remove":
        rec = find_recipient(arg)
        if not rec:
            await reply("Получатель не найден.")
        else:
            set_setting("pending", json.dumps({"type": "remove", "user_id": rec[0]}))
            await reply(f"Удалить {rec[1]}? Ответьте «да».")
    elif cmd == "/archive":
        rows = archive_rows()
        if arg.lower().startswith("find"):
            rows = filter_rows(rows, arg[4:])
        else:
            rows = rows[: int(arg) if arg.isdigit() else 10]
        await reply("\n".join(f"{r[0]}. {fmt_ts(r[1])} — {r[2]} [{r[4]}]" for r in rows) or "Записей нет.")
    elif cmd == "/resend":
        m = re.fullmatch(r"(\d+)\s+(.+)", arg)
        rows = q("SELECT id, ts, title, text, status FROM archive WHERE id = ?", (int(m.group(1)),)) if m else []
        row = rows[0] if rows else None
        if not m or not row or row[4] != "успех":
            await reply("Формат: /resend [номер] [получатель или все]. Запись должна существовать и быть успешной.")
            return
        if m.group(2).strip().lower() == "все":
            targets = active_recipients()
        else:
            rec = find_recipient(m.group(2))
            targets = [rec] if rec else []
        if not targets:
            await reply("Получатель не найден.")
            return
        set_setting("pending", json.dumps({"type": "resend", "archive_id": row[0],
                                           "targets": [list(t) for t in targets]}))
        await reply(f"Отправить повторно №{row[0]} «{row[2]}» → "
                    f"{', '.join(t[1] for t in targets)}? Ответьте «да».")
    elif cmd in ("/pause", "/resume"):
        rec = find_recipient(arg)
        if not rec:
            await reply("Получатель не найден.")
        else:
            active = 1 if cmd == "/resume" else 0
            db("UPDATE recipients SET active = ? WHERE user_id = ?", (active, rec[0]))
            await reply(f"{rec[1]}: {'включён' if active else 'отключён, рассылки не получает'}.")
    elif cmd == "/prompt":
        if arg.lower().startswith("set"):
            new = arg[3:].strip()
            if not new:
                await reply("Формат: /prompt set [новый текст]")
                return
            set_setting("pending", json.dumps({"type": "set_prompt", "text": new}))
            await reply(f"Старый промпт:\n{get_prompt()}\n\nНовый промпт:\n{new}\n\n"
                        "Заменить? Ответьте «да». Новый промпт применится к следующим изображениям.")
        else:
            await reply(get_prompt())
    elif cmd == "/status":
        n_rec = q("SELECT COUNT(*) FROM recipients")[0][0]
        n_arc = q("SELECT COUNT(*) FROM archive")[0][0]
        last = q("SELECT id, ts, title, status FROM archive ORDER BY id DESC LIMIT 1")
        last_s = f"{last[0][0]}. {fmt_ts(last[0][1])} — {last[0][2]} [{last[0][3]}]" if last else "нет"
        lines = [f"Получателей: {n_rec}", f"Записей в архиве: {n_arc}", f"Последний результат: {last_s}",
                 f"Модель: {CFG.openrouter_model}"]
        if _last_rate:
            lines.append("Лимиты OpenRouter: " + describe_limits(_last_rate))
        await reply("\n".join(lines))
    else:
        await reply("Неизвестная команда. /help — список команд.")


async def execute_pending(client: httpx.AsyncClient, owner_id: int, p: dict) -> None:
    if p["type"] == "remove":
        db("DELETE FROM recipients WHERE user_id = ?", (p["user_id"],))
        await send_text(client, owner_id, "Получатель удалён.")
    elif p["type"] == "set_prompt":
        set_setting("prompt", p["text"])
        await send_text(client, owner_id, "Промпт заменён.")
    elif p["type"] == "resend":
        rows = q("SELECT ts, title, text FROM archive WHERE id = ?", (p["archive_id"],))
        if not rows:
            await send_text(client, owner_id, "Запись уже удалена из архива.")
            return
        recs = [(t[0], t[1]) for t in p["targets"]]
        ok, failed = await deliver(client, p["archive_id"], rows[0][0], compose_text(rows[0][1], rows[0][2]), recs)
        await send_text(client, owner_id, delivery_summary(ok, len(recs), failed))


# =====================================================================
# 9. ПЕРВОНАЧАЛЬНАЯ НАСТРОЙКА (мастер)
# =====================================================================
def wizard_question(step: str) -> str:
    if step == "prompt":
        return ("Шаг 1 из 3: промпт для нейросети. Ответьте «да», чтобы принять стандартный "
                "(посмотреть: /prompt), или пришлите свой текст.")
    if step == "recipients":
        return ("Шаг 2 из 3: получатели. Присылайте имена (по одному или списком, каждое с новой строки): "
                "для каждого создам персональную ссылку. Перешлите её получателю: он откроет ссылку, "
                "нажмёт Start и подтвердит своё имя. Когда закончите, напишите «готово».")
    return "Шаг 3 из 3: проверьте настройку. Ответьте «да» для завершения."


async def setup_wizard(client: httpx.AsyncClient, uid: int, text: str) -> None:
    step = get_setting("setup_step") or "start"

    async def say(s: str) -> None:
        await send_text(client, uid, s)

    if step == "start":
        set_setting("setup_step", "prompt")
        await say("Первоначальная настройка. " + wizard_question("prompt"))
    elif step == "prompt":
        if text.lower() == "да":
            set_setting("prompt", get_setting("prompt_draft") or DEFAULT_PROMPT)
            set_setting("setup_step", "recipients")
            await say(wizard_question("recipients"))
        else:
            set_setting("prompt_draft", text)
            await say(f"Новый промпт:\n{text}\n\nОтветьте «да», чтобы принять, или пришлите другой текст.")
    elif step == "recipients":
        if text.lower() == "готово":
            set_setting("setup_step", "confirm")
            recs = "\n".join(f"{n} — {u}" for u, n in active_recipients()) or "нет"
            await say(f"Промпт:\n{get_prompt()[:300]}\n\nПолучатели:\n{recs}\n\n" + wizard_question("confirm"))
            return
        names = parse_names(text)
        if not names:
            await say("Пришлите имя получателя или напишите «готово».")
        else:
            await say(make_links(names) + "\n\nПришлите следующее имя или напишите «готово».")
    else:  # confirm
        if text.lower() == "да":
            set_setting("setup_done", "1")
            await say("Настройка завершена. Пришлите изображение. /help — список команд.")
        else:
            await say(wizard_question("confirm"))


# =====================================================================
# 10. ОБРАБОТКА СООБЩЕНИЙ ВЛАДЕЛЬЦА И ИЗОБРАЖЕНИЙ
# =====================================================================
def ai_error_text(e: Exception) -> str:
    """Сообщение владельцу об ошибке распознавания: суть и причина (секреты замаскированы)."""
    head = {
        "limit": "Лимиты модели исчерпаны, изображение не обработано. Пришлите его после сброса лимита.",
        "credits": "На счёте OpenRouter недостаточно кредитов, изображение не обработано.",
        "auth": "OpenRouter не принял ключ или доступ запрещён, изображение не обработано.",
        "model": "Модель OpenRouter не найдена или недоступна, изображение не обработано.",
    }.get(getattr(e, "kind", "other"), MSG["ai_error"])
    return f"{head}\nПричина: {redact(str(e))[:300]}"


async def process_images(client: httpx.AsyncClient, uid: int, m: dict) -> None:
    purge_archive()
    CFG.save_dir.mkdir(parents=True, exist_ok=True)
    received_at = time.time()
    mid_part = re.sub(r"[^A-Za-z0-9_-]", "_", str(m["mid"]))[:16]     # имя файла без опасных символов
    for i, p in enumerate(m["images"]):
        try:
            data, fmt = await download_image(client, p["url"])
        except ImageError as e:                      # формат или размер
            await send_text(client, uid, str(e))
            continue
        except RuntimeError:
            log.exception("скачивание mid=%s", m["mid"])
            await send_text(client, uid, MSG["no_file"])
            continue
        path = CFG.save_dir / f"{int(received_at)}_{mid_part}_{i}.{fmt}"
        path.write_bytes(data)
        log.info("картинка mid=%s принята: %s, %d байт, отправляю в нейросеть", m["mid"], fmt, len(data))
        try:
            result = await recognize(client, data, fmt)
        except (AIError, httpx.HTTPError, KeyError, IndexError, ValueError) as e:
            log.error("распознавание mid=%s не удалось: %s", m["mid"], redact(str(e)))
            archive_add(received_at, "Ошибка распознавания", "", str(path), "ошибка")
            await send_text(client, uid, ai_error_text(e))
            continue
        # заголовок записи архива — первая непустая строка ответа, тело — всё остальное
        title, body = split_title_body(result)
        aid = archive_add(received_at, title, body, str(path), "успех")
        log.info("распознано: запись архива №%d, заголовок «%s», тело %d знаков", aid, title[:60], len(body))
        recs = active_recipients()
        if not recs:
            await send_text(client, uid, MSG["no_recipients"])
            continue
        ok, failed = await deliver(client, aid, received_at, compose_text(title, body), recs)
        log.info("рассылка записи №%d: доставлено %d из %d", aid, ok, len(recs))
        await send_text(client, uid, delivery_summary(ok, len(recs), failed))


_user_locks: dict[int, asyncio.Lock] = {}


async def handle_owner(client: httpx.AsyncClient, uid: int, m: dict) -> None:
    """Порядок: подтверждение «да» -> команды «/» -> настройка -> изображения -> «Нужно прислать изображение»."""
    text = m["text"]
    lock = _user_locks.setdefault(uid, asyncio.Lock())
    async with lock:                                 # тексты и настройка идут строго по очереди
        pending = get_setting("pending")
        if pending:                                  # 1. подтверждение ожидающего действия
            del_setting("pending")
            if text.lower() == "да":
                await execute_pending(client, uid, json.loads(pending))
                return
            await send_text(client, uid, MSG["cancelled"])   # любой другой ответ отменяет действие
        if text.startswith("/"):                     # 2. команды (даже если приложена картинка)
            await handle_owner_command(client, uid, text)
            return
        if get_setting("setup_done") != "1":         # 3. первоначальная настройка
            if text and not m["images"]:
                await setup_wizard(client, uid, text)
            else:
                await send_text(client, uid, MSG["setup_required"])
                step = get_setting("setup_step") or "start"
                if step == "start":
                    await setup_wizard(client, uid, "")
                else:
                    await send_text(client, uid, wizard_question(step))
            return
        if not m["images"]:                          # 5. текст без «/» и без картинки
            await send_text(client, uid, MSG["need_image"])
            return
    await process_images(client, uid, m)             # 4. изображения (вне замка: ИИ долго, команды не ждут)


# =====================================================================
# 11. ОБРАБОТКА ОДНОГО ОБНОВЛЕНИЯ (фоновая задача)
# =====================================================================
def stranger_text(uid: int, name: str) -> str:
    """Сообщение владельцу о незнакомом пользователе, который написал боту без приглашения."""
    who = f", имя «{name}»" if name else ""
    return (f"Незнакомый пользователь (без приглашения): ID {uid}{who}. "
            "Если это получатель, создайте ему ссылку: /add Имя")


async def process(update: dict, client: httpx.AsyncClient, replayed: bool = False) -> None:
    """replayed=True: обновление восстановлено из inbox после сбоя. Его mid мог остаться
    в seen (процесс упал до освобождения), поэтому проверку дубликата для него пропускаем."""
    if update.get("update_type") == "bot_started":
        uid = (update.get("user") or {}).get("user_id")
        payload = (update.get("payload") or "").strip()
        log.info("bot_started: user_id=%s (chat_id=%s)%s", uid, update.get("chat_id"),
                 " по ссылке-приглашению" if payload else "")
        if uid is not None and payload and uid != CFG.owner_id:
            await handle_invite_start(client, uid, payload, display_name(update.get("user")))
            return
        if uid is not None:
            first = not has_started(uid)
            mark_started(uid)
            if uid != CFG.owner_id:
                rec = find_recipient(str(uid))
                if rec and first:
                    await alert_owner(f"{rec[1]} (ID {uid}) нажал(а) Start: теперь получает рассылку.",
                                      key=f"start:{uid}", every=3600.0)
                elif not rec:
                    await alert_owner(stranger_text(uid, display_name(update.get("user"))),
                                      key=f"stranger:{uid}", every=86400.0)
        return
    m = parse_message(update)
    if not m or not m["mid"] or m["sender_id"] is None:
        return
    mid, uid = m["mid"], m["sender_id"]

    if uid == CFG.owner_id:
        if not claim_mid(mid) and not replayed:
            log.info("дубликат mid=%s", mid)
            return
        try:
            await handle_owner(client, uid, m)
        except BaseException as e:                   # в т. ч. отмена при остановке: при повторе событие не будет потеряно
            release_mid(mid)
            if isinstance(e, Exception):
                log.exception("обработка mid=%s не удалась", mid)
                await alert_owner(f"Ошибка при обработке вашего сообщения: {type(e).__name__}: {e}"[:500],
                                  key="owner_err", every=300.0)
                return
            raise
    elif is_recipient(uid):
        mark_started(uid)                            # написал боту, значит диалог открыт
        if claim_mid(mid) or replayed:
            try:
                await handle_recipient_request(client, uid, m["text"])
            except BaseException as e:
                release_mid(mid)
                if isinstance(e, Exception):
                    log.exception("запрос получателя mid=%s не удался", mid)
                    await alert_owner(f"Ошибка при обработке запроса получателя ID {uid}: "
                                      f"{type(e).__name__}: {e}"[:500], key=f"recipient_err:{uid}", every=300.0)
                    return
                raise
        else:
            log.info("дубликат mid=%s", mid)
    else:
        if await handle_invite_reply(client, uid, m["text"]):   # ответ «да» на вопрос «Вы — <имя>?»
            return
        log.info("чужой отправитель user_id=%s", uid)
        if first_time_stranger(uid):
            await send_text(client, uid, MSG["not_invited"])
        await alert_owner(stranger_text(uid, m["sender_name"]), key=f"stranger:{uid}", every=86400.0)


# =====================================================================
# 12. LONG POLLING
# =====================================================================
class AuthError(Exception):
    """401: токен неверен или отозван. Повторы бессмысленны."""


class TransientApiError(Exception):
    def __init__(self, msg: str, retry_after: Optional[float] = None):
        super().__init__(msg)
        self.retry_after = retry_after


_tasks: set[asyncio.Task] = set()


def spawn(coro: Awaitable[None], inbox_id: Optional[int] = None) -> asyncio.Task:
    """Фоновая задача на одно обновление. Ошибка задачи не роняет цикл опроса.
    Запись входящей очереди помечается выполненной, только если задача дошла до конца
    (при отмене или сбое она останется и будет обработана заново при следующем старте)."""
    async def runner() -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            # Ошибка повторяется при каждом запуске, если не отметить запись выполненной.
            # Поэтому обновление отмечаем обработанным и пишем в журнал, что оно отброшено.
            log.exception("обновление отброшено из-за необработанной ошибки (inbox_id=%s)", inbox_id)
            await alert_owner("Необработанная ошибка при обработке события, событие отброшено. "
                              "Подробности в журнале.", key="task_err", every=300.0)
        if inbox_id is not None:
            inbox_done(inbox_id)
    t = asyncio.create_task(runner())
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)
    return t


def backoff_delay(n: int, retry_after: Optional[float] = None) -> float:
    """1, 2, 4, ... секунд до BACKOFF_MAX с разбросом -20%. Retry-After сервера главнее."""
    wait = min(BACKOFF_MAX, 2.0 ** n) * random.uniform(0.8, 1.0)
    if retry_after:
        wait = max(wait, min(retry_after, BACKOFF_MAX))
    return wait


async def get_updates(client: httpx.AsyncClient, marker: Optional[int]) -> dict:
    params: dict[str, Any] = {"limit": CFG.poll_limit, "timeout": CFG.poll_timeout, "types": POLL_TYPES}
    if marker is not None:
        params["marker"] = marker
    r = await client.get(f"{API}/updates", params=params,
                         headers={"Authorization": CFG.bot_token},
                         timeout=CFG.poll_timeout + 15)
    if r.status_code == 401:
        raise AuthError(r.text[:200])
    if r.status_code == 429 or r.status_code >= 500:
        raise TransientApiError(f"HTTP {r.status_code}", parse_retry_after(r.headers.get("Retry-After", "")))
    if r.status_code != 200:                         # 400/404/405 и т. п.
        raise TransientApiError(f"HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    if not isinstance(data, dict):
        raise TransientApiError("неожиданный формат ответа")
    return data


def replay_inbox(client: httpx.AsyncClient) -> int:
    """При старте заново запускает обновления, сохранённые, но не обработанные
    (падение или остановка посреди обработки). Дубли отсекаются по mid."""
    pend = inbox_pending()
    for iid, upd in pend:
        spawn(process(upd, client, replayed=True), iid)
    if pend:
        log.warning("восстановлено необработанных обновлений: %d", len(pend))
    return len(pend)


async def poll_loop(client: httpx.AsyncClient, max_polls: Optional[int] = None) -> None:
    """Бесконечный опрос. max_polls нужен только тестам.

    Порядок защиты от пропуска событий: получили пачку -> в одной транзакции записали
    её в inbox и сдвинули marker -> запустили обработку. Если процесс упадёт после этого,
    события возьмутся из inbox; если до — marker не сдвинут, MAX отдаст их снова."""
    marker = load_marker()
    log.info("long polling запущен, marker=%s, timeout=%s, limit=%s", marker, CFG.poll_timeout, CFG.poll_limit)
    fails = 0
    polls = 0
    while True:
        started = time.monotonic()
        try:
            data = await get_updates(client, marker)
        except AuthError as e:
            raise SystemExit(f"401: токен MAX_BOT_TOKEN неверен или отозван ({e})")
        except TransientApiError as e:
            wait = backoff_delay(fails, e.retry_after)
            fails += 1
            log.warning("опрос: %s, пауза %.1f с (подряд ошибок: %d)", e, wait, fails)
            if fails >= 5:
                await alert_owner(f"Опрос MAX: {fails} ошибок подряд, последняя: {e}. Бот продолжает попытки.",
                                  key="poll_fail", every=3600.0)
            await _sleep(wait)
            continue
        except (httpx.HTTPError, ValueError) as e:   # сеть, таймаут, битый JSON
            wait = backoff_delay(fails)
            fails += 1
            log.warning("опрос: %r, пауза %.1f с (подряд ошибок: %d)", e, wait, fails)
            if fails >= 5:
                await alert_owner(f"Опрос MAX: {fails} ошибок подряд, последняя: {e!r}. Бот продолжает попытки.",
                                  key="poll_fail", every=3600.0)
            await _sleep(wait)
            continue
        fails = 0

        updates = [u for u in (data.get("updates") or []) if isinstance(u, dict)]
        raw_marker = data.get("marker")
        try:
            new_marker = int(raw_marker) if raw_marker is not None else None
        except (TypeError, ValueError):
            log.warning("marker неожиданного вида %r: игнорирую", raw_marker)
            new_marker = None
        if new_marker is not None and marker is not None and new_marker < marker:
            log.warning("marker уменьшился (%s -> %s): сдвиг назад игнорирую", marker, new_marker)
            new_marker = None
        ids = inbox_store(updates, new_marker if new_marker != marker else None)
        if new_marker is not None:
            marker = new_marker
        for upd, iid in zip(updates, ids):
            spawn(process(upd, client), iid)
        if updates:
            log.info("получено обновлений: %d, marker=%s", len(updates), marker)
        elif time.monotonic() - started < 0.2:       # сервер ответил мгновенно и пусто: не крутим CPU
            await _sleep(1.0)

        polls += 1
        if max_polls is not None and polls >= max_polls:
            return


async def drain_tasks(grace: float = SHUTDOWN_GRACE) -> None:
    """При остановке даём фоновым задачам закончить, потом отменяем оставшиеся."""
    pending = [t for t in _tasks if not t.done()]
    if not pending:
        return
    log.info("жду завершения задач: %d (до %.0f с)", len(pending), grace)
    _, still = await asyncio.wait(pending, timeout=grace)
    for t in still:
        t.cancel()
    if still:
        await asyncio.gather(*still, return_exceptions=True)
        log.warning("отменено задач: %d (они останутся во входящей очереди и повторятся при запуске)", len(still))


# =====================================================================
# 13. ПРОВЕРКА И СНЯТИЕ WEBHOOK
# =====================================================================
async def check_webhook(client: httpx.AsyncClient) -> None:
    """Пока активна подписка webhook, GET /updates событий не отдаёт. Проверяем GET /subscriptions."""
    hdr = {"Authorization": CFG.bot_token}
    try:
        r = await client.get(f"{API}/subscriptions", headers=hdr, timeout=15)
    except httpx.HTTPError as e:
        log.warning("не удалось проверить подписки webhook (%r). Если бот молчит, "
                    "проверьте вручную GET /subscriptions.", e)
        return
    if r.status_code == 401:
        raise SystemExit("401 от MAX: токен MAX_BOT_TOKEN неверен или отозван")
    if r.status_code != 200:
        log.warning("GET /subscriptions вернул %s: проверка webhook пропущена", r.status_code)
        return
    try:
        subs = (r.json() or {}).get("subscriptions") or []
    except ValueError:
        log.warning("GET /subscriptions: ответ не JSON, проверка webhook пропущена")
        return
    if not subs:
        log.info("подписок webhook нет, long polling возможен")
        return
    urls = [s.get("url") for s in subs]
    if not CFG.unsubscribe_webhook:
        raise SystemExit(
            "У бота активна подписка webhook: " + ", ".join(map(str, urls)) + ". Пока она есть, "
            "long polling не получает события. Задайте MAX_UNSUBSCRIBE_WEBHOOK=1, чтобы скрипт "
            "сам снял подписку (DELETE /subscriptions), или отпишитесь вручную. "
            "Внимание: после этого webhook перестанет работать.")
    for u in urls:
        d = await client.delete(f"{API}/subscriptions", params={"url": u}, headers=hdr, timeout=15)
        try:
            ok = d.status_code == 200 and (d.json() or {}).get("success") is True
        except ValueError:
            ok = False
        log.warning("отписка от webhook %s: %s", u, "успешно" if ok else f"НЕ УДАЛАСЬ ({d.status_code} {d.text[:200]})")
        if not ok:
            raise SystemExit("Не удалось снять подписку webhook, long polling не заработает.")


# =====================================================================
# 14. ЗАПУСК
# =====================================================================
async def amain() -> None:
    global _http
    init_db()
    purge_archive()
    loop = asyncio.get_running_loop()
    main_task = asyncio.current_task()
    for sig in (signal.SIGINT, signal.SIGTERM):     # Linux/macOS; на Windows Ctrl+C ловит asyncio.run
        try:
            loop.add_signal_handler(sig, main_task.cancel)   # type: ignore[union-attr]
        except (NotImplementedError, RuntimeError, ValueError):
            pass
    async with httpx.AsyncClient() as client:
        _http = client
        try:
            await check_webhook(client)
            await check_model_vision(client)
            restored = replay_inbox(client)
            await alert_owner(f"Бот запущен. Модель: {CFG.openrouter_model}. "
                              f"Получателей: {len(active_recipients())}. Восстановлено событий: {restored}.",
                              force=True)
            await poll_loop(client)
        finally:
            await drain_tasks()
            _http = None
    log.info("остановлено")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        load_config()
    except ConfigError as e:
        print(f"ОШИБКА НАСТРОЙКИ: {e}", file=sys.stderr)
        return 2
    try:
        asyncio.run(amain())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("остановлено по Ctrl+C / SIGTERM")
    except SystemExit as e:
        if e.code not in (None, 0):
            log.error("%s", redact(str(e.code)))
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
