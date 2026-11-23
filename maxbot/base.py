"""Конфигурация, журнал, общие функции текста и времени."""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Optional
from zoneinfo import ZoneInfo

try:
    # MAX и некоторые другие российские сервисы используют сертификат,
    # выпущенный отечественным удостоверяющим центром (НУЦ Минцифры), которому
    # certifi не доверяет по умолчанию. Этот пакет учит httpx доверять тем же
    # сертификатам, что и операционная система (после установки сертификатов
    # Минцифры в Windows — см. предупреждение в load_config() ниже).
    import httpx_system_certs  # noqa: F401
    _HTTPX_SYSTEM_CERTS = True
except ImportError:
    _HTTPX_SYSTEM_CERTS = False


log = logging.getLogger("max_polling")

# =====================================================================
# 1. НАСТРОЙКИ: разбор .env, проверка обязательных переменных, маскировка секретов
# =====================================================================
SCRIPT_DIR = Path(__file__).resolve().parent.parent

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
    openrouter_max_tokens: int = 8192
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
        openrouter_max_tokens=_int(env, "OPENROUTER_MAX_TOKENS", 8192, 1, 128000),
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
    if not _HTTPX_SYSTEM_CERTS:
        log.warning("пакет httpx_system_certs не установлен: если сервер использует сертификат НУЦ Минцифры, "
                    "запросы будут падать с CERTIFICATE_VERIFY_FAILED. Установите: pip install httpx-system-certs")
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

AI_RETRY_PAUSES = (2.0, 3.0, 5.0, 8.0)   # паузы перед повтором: 429, 5xx, сеть, таймаут

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
    "already_done": "Данное изображение уже обработано",
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
