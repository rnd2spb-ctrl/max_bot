"""Отправка сообщений MAX с паузами и повтором, оповещение владельца."""
from __future__ import annotations

import asyncio
import re
import time
from html.parser import HTMLParser
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import Awaitable, Callable, Mapping, Optional
import httpx
from . import base
from .base import API, MSG_LIMIT, log, redact, split_text


_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep   # подменяется в тестах

# ---------- очередь отправки: не более 2 сообщений в секунду в один чат ----------
_last_send: dict[int, float] = {}

_send_locks: dict[int, asyncio.Lock] = {}

SEND_ATTEMPTS = 5


class _VisibleTextParser(HTMLParser):
    """Извлекает отображаемый текст из HTML-сообщения MAX для журнала."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() == "br":
            self.parts.append("\n")


def _chat_label(user_id: int) -> str:
    """Определяет адресата исходящего сообщения без обращения к сетевому API."""
    if user_id == base.CFG.owner_id:
        return f"владелец user_id={user_id}"

    try:
        # Локальный импорт нужен, чтобы не создавать циклическую зависимость при старте.
        from .storage import q

        rows = q("SELECT name FROM recipients WHERE user_id = ?", (user_id,))
        if rows:
            return f"получатель «{rows[0][0]}» user_id={user_id}"

        rows = q(
            "SELECT name FROM invites WHERE user_id = ? "
            "AND status = 'ждёт подтверждения' ORDER BY id DESC LIMIT 1",
            (user_id,),
        )
        if rows:
            return f"пользователь по приглашению «{rows[0][0]}» user_id={user_id}"
    except Exception:
        # Журналирование не должно мешать самой отправке, например до открытия БД.
        pass

    return f"user_id={user_id}"


def _visible_message_text(text: str, message_format: Optional[str]) -> str:
    """Текст в журнале соответствует видимому содержимому сообщения; секреты скрываются."""
    visible = text
    if message_format == "html":
        parser = _VisibleTextParser()
        try:
            parser.feed(text)
            visible = "".join(parser.parts)
        except Exception:
            visible = re.sub(r"<[^>]*>", "", text)

    visible = redact(visible)
    # Токены одноразовых ссылок не должны сохраняться в лог-файлах.
    visible = re.sub(
        r"(https?://max\.ru/[^\s<>\"]+\?start=)[^\s&<>\"]+",
        lambda match: match.group(1) + "[ТОКЕН СКРЫТ]",
        visible,
        flags=re.IGNORECASE,
    )
    return visible


def _log_outgoing_message(
    user_id: int, text: str, message_format: Optional[str], status: str,
) -> None:
    """Пишет в журнал именно текст, переданный MAX, и результат его отправки."""
    recipient = _chat_label(user_id)
    visible = _visible_message_text(text, message_format)
    indented = "\n".join("    " + line for line in visible.splitlines()) or "    [сообщение без текста]"
    if status == "ok":
        log.info("исходящее сообщение доставлено → %s:\n%s", recipient, indented)
    else:
        log.warning(
            "исходящее сообщение не доставлено → %s (статус=%s):\n%s",
            recipient, status, indented,
        )


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


async def _post_message(client: httpx.AsyncClient, user_id: int, body: dict) -> str:
    """Общая отправка POST /messages с паузами (0.5 с на чат) и повтором. Используется
    и для текста (send_message), и для картинки (send_image) -- тело запроса разное,
    логика повторов и лимитов одна. Возвращает «ok», «rejected» (4xx, кроме 429: чаще
    всего пользователь не нажал Start) или «failed» (сеть/429/5xx после повторов)."""
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
                    headers={"Authorization": base.CFG.bot_token},
                    json=body,
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


async def send_message(client: httpx.AsyncClient, user_id: int, text: str,
                       attachments: Optional[list[dict]] = None,
                       format: Optional[str] = None) -> str:
    """Одно сообщение, при желании с вложениями и форматированием MAX.
    Возвращает «ok», «rejected» или «failed» -- см. _post_message."""
    body: dict = {"text": text[:MSG_LIMIT]}
    if attachments:
        body["attachments"] = attachments
    if format:
        body["format"] = format
    status = await _post_message(client, user_id, body)
    _log_outgoing_message(user_id, body["text"], format, status)
    return status


async def send_image(client: httpx.AsyncClient, user_id: int, image_payload: dict) -> str:
    """Отправляет картинку как вложение. image_payload -- то, что вернула upload_image()
    (max_api.py): {"token": ...} либо {"photos": {...}}. Текста при этом не посылаем."""
    return await _post_message(client, user_id, {"attachments": [{"type": "image", "payload": image_payload}]})


async def answer_callback(
    client: httpx.AsyncClient,
    callback_id: str,
    text: Optional[str] = None,
    attachments: Optional[list[dict]] = None,
    remove_attachments: bool = True,
) -> bool:
    """Отвечает на нажатие inline-кнопки (POST /answers).

    attachments=None и remove_attachments=False сохраняют текущую клавиатуру.
    Пустой список attachments удаляет все вложения исходного сообщения.
    """
    message: dict = {}
    if text is not None:
        message["text"] = text[:MSG_LIMIT]
    if attachments is not None:
        message["attachments"] = attachments
    elif remove_attachments:
        message["attachments"] = []
    body = {"message": message}
    try:
        r = await client.post(f"{API}/answers", params={"callback_id": callback_id},
                              headers={"Authorization": base.CFG.bot_token}, json=body, timeout=15)
    except httpx.HTTPError as e:
        log.warning("ответ на callback %s: сетевая ошибка %r", callback_id, e)
        return False
    if r.status_code != 200:
        log.warning("ответ на callback %s: HTTP %s %s", callback_id, r.status_code, r.text[:200])
        return False
    return True


async def send_text(client: httpx.AsyncClient, user_id: int, text: str,
                    format: Optional[str] = None) -> str:
    """Длинный текст уходит частями до 4000 символов с пометками «(1/N)».
    При необходимости включает форматирование MAX. При сбое части — стоп."""
    for part in split_text(text):
        status = await send_message(client, user_id, part, format=format)
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
    if _http is None or not base.CFG.owner_id:
        return
    try:
        status = await send_text(_http, base.CFG.owner_id, text)
        if status != "ok":
            log.warning("служебное сообщение владельцу не доставлено (%s)", status)
    except Exception:
        log.exception("служебное сообщение владельцу: сбой отправки")
