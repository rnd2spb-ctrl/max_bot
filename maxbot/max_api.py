"""API MAX: long polling, разбор сообщений, загрузка картинок."""
from __future__ import annotations

import struct
from typing import Any, Optional
import httpx
from . import base, transport
from .base import API, MSG, POLL_TYPES, log
from .transport import parse_retry_after


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
                    if len(buf) > base.CFG.max_image_bytes:
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
                await transport._sleep(5)
    raise RuntimeError(f"Не удалось скачать: {last}")


# =====================================================================
# 12. LONG POLLING
# =====================================================================
class AuthError(Exception):
    """401: токен неверен или отозван. Повторы бессмысленны."""


class TransientApiError(Exception):
    def __init__(self, msg: str, retry_after: Optional[float] = None):
        super().__init__(msg)
        self.retry_after = retry_after


async def get_updates(client: httpx.AsyncClient, marker: Optional[int]) -> dict:
    params: dict[str, Any] = {"limit": base.CFG.poll_limit, "timeout": base.CFG.poll_timeout, "types": POLL_TYPES}
    if marker is not None:
        params["marker"] = marker
    r = await client.get(f"{API}/updates", params=params,
                         headers={"Authorization": base.CFG.bot_token},
                         timeout=base.CFG.poll_timeout + 15)
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


# =====================================================================
# 13. ПРОВЕРКА И СНЯТИЕ WEBHOOK
# =====================================================================
async def check_webhook(client: httpx.AsyncClient) -> None:
    """Пока активна подписка webhook, GET /updates событий не отдаёт. Проверяем GET /subscriptions."""
    hdr = {"Authorization": base.CFG.bot_token}
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
    if not base.CFG.unsubscribe_webhook:
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
