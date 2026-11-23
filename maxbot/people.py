"""Получатели, персональные приглашения и доставка результатов."""
from __future__ import annotations

import hashlib
import secrets
import time
from pathlib import Path
from typing import Optional
import httpx
from . import base
from .base import MSG, MSG_LIMIT, compose_text, fmt_ts, log, parse_date
from .max_api import ImageUploadError, upload_image
from .storage import archive_rows, db, filter_rows, q
from .transport import alert_owner, send_image, send_text


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
       (name, hash_token(token), now, now + base.CFG.invite_ttl_hours * 3600))
    return token


def invite_link(token: str) -> str:
    return f"https://max.ru/{base.CFG.bot_name}?start={token}"


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


async def send_links(client: httpx.AsyncClient, uid: int, names: list[str]) -> None:
    """Создаёт приглашения и одним отчётом показывает успешные ссылки и ошибки.

    Сырые токены не сохраняются в БД и попадают только в этот ответ владельцу.
    Функция общая для /add, /newlink и мастера первоначальной настройки.
    """
    if not base.CFG.bot_name:
        await send_text(client, uid, "Ссылки не созданы: не задано имя бота. Впишите в файл .env строку "
                        "MAX_BOT_NAME=имя_бота (имя из адреса https://max.ru/имя_бота) и перезапустите бота.")
        return

    created: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []
    for name in names:
        if len(name) > 40:
            failed.append((name, "слишком длинное имя (до 40 знаков)"))
            continue
        busy = name_busy(name)
        if busy:
            failed.append((name, busy))
            continue
        link = invite_link(create_invite(name))
        created.append((name, link))

    # MAX не поддерживает таблицы как отдельный тип разметки. Используем
    # компактные строки с колонками и Markdown-ссылками, которые остаются кликабельными.
    header = "**Персональные ссылки**\n\n№ │ Имя │ Ссылка\n"
    rows = []
    for number, (name, link) in enumerate(created, 1):
        safe_name = (name.replace("\\", "\\\\").replace("|", "\\|")
                     .replace("*", "\\*").replace("_", "\\_").replace("`", "\\`"))
        rows.append(f"{number} │ {safe_name} │ [{link}]({link})")
    if created:
        rows.append("")
        rows.append(f"Создано ссылок: **{len(created)}**")
        rows.append(f"Срок действия: {base.CFG.invite_ttl_hours} ч. Каждая ссылка одноразовая.")
        rows.append("Перешлите ссылки соответствующим получателям. Потеряли ссылку — /newlink Имя.")
        rows.append("Статусы приглашений — /links.")

    if failed:
        rows.extend(["", "**Не удалось создать ссылки:**"])
        rows.extend(f"• {name}: {reason}" for name, reason in failed)

    if not rows:
        return

    # Разбиваем отчёт только между строками, чтобы ссылки не обрезались.
    chunks: list[str] = []
    current = header
    for row in rows:
        candidate = current + row + "\n"
        if len(candidate) > MSG_LIMIT - 100 and current != header:
            chunks.append(current.rstrip())
            current = header + row + "\n"
        else:
            current = candidate
    if current.strip():
        chunks.append(current.rstrip())

    for chunk in chunks:
        await send_text(client, uid, chunk, format="markdown")


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


async def deliver_image(client: httpx.AsyncClient, archive_id: int, image_path: str,
                        recs: list[tuple[int, str]]) -> tuple[int, list[str]]:
    """Рассылает исходную картинку (кнопка «Картинка»): файл загружается в MAX один раз
    (upload_image), затем получателям уходит ссылка на этот же загруженный файл. Если
    загрузка не удалась (например, файл уже удалён по сроку хранения архива) -- все
    получатели считаются недоставленными, в журнал пишется причина."""
    ok, failed = 0, []
    try:
        image_payload = await upload_image(client, Path(image_path))
    except (ImageUploadError, OSError) as e:
        log.error("рассылка картинки записи №%d: загрузка не удалась: %s", archive_id, e)
        for uid, name in recs:
            db("INSERT INTO deliveries (archive_id, recipient_id, status, ts) VALUES (?, ?, ?, ?)",
               (archive_id, uid, "fail", time.time()))
            failed.append(name)
        return 0, failed
    for uid, name in recs:
        status = await send_image(client, uid, image_payload)
        db("INSERT INTO deliveries (archive_id, recipient_id, status, ts) VALUES (?, ?, ?, ?)",
           (archive_id, uid, "ok" if status == "ok" else "fail", time.time()))
        if status == "ok":
            ok += 1
        else:
            failed.append(name)
            log.warning("не доставлена картинка %s (%s): %s", name, uid,
                       MSG["not_started"] if status == "rejected" and not has_started(uid) else MSG["send_failed"])
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
