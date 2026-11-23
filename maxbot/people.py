"""Получатели, персональные приглашения и доставка результатов."""
from __future__ import annotations

import hashlib
import html
import secrets
import time
from pathlib import Path
from typing import Optional
import httpx
from . import base
from .base import MSG, compose_text, fmt_ts, log, parse_date
from .max_api import upload_image
from .storage import (
    archive_rows, claim_archive_dispatch, db, dispatch_recipients, filter_rows,
    active_recipient_ids, finish_archive_dispatch, mark_dispatch_recipient_sending,
    q, recipients_for_org, record_dispatch_delivery, touch_archive_dispatch,
)
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


def create_invite(name: str, org: int = 3) -> str:
    """Создаёт одноразовую ссылку с кодом организации."""
    if org not in (1, 2, 3):
        raise ValueError("org должен быть 1, 2 или 3")
    token = secrets.token_urlsafe(16)
    now = time.time()
    db("INSERT INTO invites (name, token_hash, created_ts, expires_ts, status, org) "
       "VALUES (?, ?, ?, ?, 'ожидает', ?)",
       (name, hash_token(token), now, now + base.CFG.invite_ttl_hours * 3600, org))
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


def parse_names_with_org(text: str) -> list[tuple[str, int]]:
    """Каждая строка — имя и необязательный последний код организации 1/2/3."""
    out: list[tuple[str, int]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        head, _, tail = line.rpartition(" ")
        if head and tail in ("1", "2", "3"):
            out.append((head.strip(), int(tail)))
        else:
            out.append((line, 3))
    return out


async def send_links(
    client: httpx.AsyncClient, uid: int, names: list[tuple[str, int]]
) -> None:
    """Создаёт приглашения и одним отчётом показывает успешные ссылки и ошибки.

    Сырые токены не сохраняются в БД и попадают только в этот ответ владельцу.
    Функция общая для /add, /newlink и мастера первоначальной настройки.
    """
    if not base.CFG.bot_name:
        await send_text(client, uid, "Ссылки не созданы: не задано имя бота. Впишите в файл .env строку "
                        "MAX_BOT_NAME=имя_бота (имя из адреса https://max.ru/имя_бота) и перезапустите бота.")
        return

    created: list[tuple[str, int, str]] = []
    failed: list[tuple[str, str]] = []
    warnings: list[str] = []
    org_labels = {1: "Озон", 2: "WB", 3: "Озон и WB"}
    for name, org in names:
        tail = name.rpartition(" ")[2]
        if tail.isdigit() and tail not in ("1", "2", "3"):
            warnings.append(
                f"{name}: последний числовой фрагмент похож на ошибочный код; "
                "имя сохранено целиком, назначены обе организации"
            )
        if len(name) > 40:
            failed.append((name, "слишком длинное имя (до 40 знаков)"))
            continue
        busy = name_busy(name)
        if busy:
            failed.append((name, busy))
            continue
        link = invite_link(create_invite(name, org))
        created.append((name, org, link))

    # Каждый получатель: жирные номер и имя, затем полная кликабельная ссылка
    # на отдельной строке. Между получателями — пустая строка; шапки и разделителей нет.
    rows = []
    for number, (name, org, link) in enumerate(created, 1):
        safe_name = html.escape(name)
        safe_link = html.escape(link, quote=True)
        rows.append(
            f"<b>{number:02d} {safe_name} — {org_labels[org]}</b>\n"
            f'<a href="{safe_link}">{html.escape(link)}</a>'
        )

    if warnings:
        if rows:
            rows.append("")
        rows.append("<b>Проверьте код организации:</b>")
        rows.extend(f"• {html.escape(warning)}" for warning in warnings)

    if failed:
        if rows:
            rows.append("")
        rows.append("<b>Не удалось создать ссылки:</b>")
        rows.extend(
            f"• {html.escape(name)}: {html.escape(reason)}"
            for name, reason in failed
        )

    if not rows:
        return

    # send_text разбивает длинный отчёт по абзацам; каждая запись короче лимита MAX.
    await send_text(client, uid, "\n\n".join(rows), format="html")


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


async def dispatch_archive(
    client: httpx.AsyncClient,
    archive_id: int,
    kind: Optional[str],
    recs: list[tuple[int, str]],
    expires_ts: Optional[float],
    *,
    allow_expired: bool = False,
    extend_recipients: bool = False,
) -> dict:
    """Запускает/продолжает рассылку, закрепляя исходный формат для archive_id.

    Получатели со статусом ok не отправляются повторно. Для изображения загрузка
    выполняется один раз на попытку рассылки; ошибка загрузки не меняет статусы
    отдельных получателей и позволяет повторить ту же кнопку.
    """
    claim = claim_archive_dispatch(
        archive_id, kind, recs, expires_ts,
        allow_expired=allow_expired,
        extend_recipients=extend_recipients,
        default_kind="text",
    )
    if claim["state"] != "claimed":
        return claim

    # claim_archive_dispatch может вернуть старые строки dispatch_recipients.
    # Сверяем их с текущими активными получателями и актуальной организацией.
    claimed_ids = [uid for uid, _, _ in claim["recipients"]]
    eligible = active_recipient_ids(claimed_ids)
    current_targets = {uid for uid, _ in recs}
    eligible.intersection_update(current_targets)

    target_ids = {uid for uid, _ in recs} if extend_recipients else None

    def scope_result(result: dict) -> dict:
        """Для /resend показывает статистику только по явно выбранным адресатам."""
        if target_ids is not None:
            selected = [
                row for row in dispatch_recipients(archive_id)
                if row[0] in target_ids
            ]
            result["total"] = len(selected)
            result["ok"] = sum(1 for row in selected if row[2] == "ok")
            result["failed"] = [row[1] for row in selected if row[2] != "ok"]
        result["kind"] = claim["kind"]
        return result

    lease_token = claim["lease_token"]
    if not eligible:
        result = finish_archive_dispatch(
            archive_id, lease_token,
            "Нет активных получателей, подходящих для этой организации",
        )
        return scope_result({
            "state": "no_active_recipients",
            "ok": result["ok"],
            "total": result["total"],
            "failed": result["failed"],
        })
    rows = q(
        "SELECT ts, title, text, image_path FROM archive WHERE id = ?",
        (archive_id,)
    )
    if not rows:
        finish_archive_dispatch(archive_id, lease_token, "Материал удалён из архива")
        return {"state": "missing"}

    ts, title, body, image_path = rows[0]
    image_payload = None
    if claim["kind"] == "image":
        if not touch_archive_dispatch(archive_id, lease_token):
            return {"state": "lease_lost", "kind": claim["kind"]}
        try:
            image_payload = await upload_image(client, Path(image_path))
        except Exception as e:
            error = f"{type(e).__name__}: ошибка загрузки изображения"
            log.error("не удалось загрузить картинку архива №%d (%s)", archive_id, type(e).__name__)
            result = finish_archive_dispatch(archive_id, lease_token, error)
            return scope_result({
                "state": "upload_failed",
                "error": error,
                "ok": result["ok"],
                "total": result["total"],
                "failed": result["failed"],
            })

    try:
        for uid, name, _previous_status in claim["recipients"]:
            if uid not in eligible:
                log.info(
                    "пропуск получателя %s (%s): удалён, приостановлен или больше не входит в организацию материала №%d",
                    name, uid, archive_id,
                )
                continue
            if not touch_archive_dispatch(archive_id, lease_token):
                return {"state": "lease_lost", "kind": claim["kind"]}
            # Повторная проверка непосредственно перед отправкой закрывает паузу/удаление,
            # произошедшие уже после первоначальной проверки списка.
            if uid not in active_recipient_ids([uid]):
                continue
            if not mark_dispatch_recipient_sending(archive_id, uid, lease_token):
                continue

            error = None
            try:
                if claim["kind"] == "text":
                    message = f"Результат от {fmt_ts(ts)}:\n{compose_text(title, body)}"
                    status = await send_text(client, uid, message)
                else:
                    status = await send_image(client, uid, image_payload)
            except Exception as e:
                status = "failed"
                error = f"{type(e).__name__}: {e}"[:500]
                log.exception("рассылка архива №%d получателю %s (%s) завершилась исключением",
                              archive_id, name, uid)

            if status != "ok" and error is None:
                if status == "rejected" and not has_started(uid):
                    error = MSG["not_started"]
                else:
                    error = MSG["send_failed"]
            record_dispatch_delivery(
                archive_id, uid, lease_token,
                "ok" if status == "ok" else "fail", error
            )
            if status != "ok":
                log.warning("не доставлено %s (%s), архив №%d: %s", name, uid, archive_id, error)
    except Exception as e:
        error = f"{type(e).__name__}: {e}"[:500]
        log.exception("ошибка в обработке рассылки архива №%d", archive_id)
        try:
            result = finish_archive_dispatch(archive_id, lease_token, error)
        except RuntimeError:
            return {"state": "lease_lost", "kind": claim["kind"]}
        result["error"] = error
        return scope_result(result)

    result = finish_archive_dispatch(archive_id, lease_token)
    return scope_result(result)


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
    row = q("SELECT id, name, expires_ts, org FROM invites WHERE user_id = ? AND status = 'ждёт подтверждения'", (uid,))
    if not row:
        return False
    inv_id, name, expires, org = row[0]
    now = time.time()
    if expires < now:
        db("UPDATE invites SET status = 'истекла' WHERE id = ?", (inv_id,))
        await send_text(client, uid, "Срок ссылки истёк. Попросите отправителя прислать новую.")
        return True
    if text.strip().lower() != "да":
        await send_text(client, uid, f"Чтобы подтвердить, что вы — {name}, ответьте «да».")
        return True
    db("UPDATE invites SET status = 'подключён', used_ts = ? WHERE id = ?", (now, inv_id))
    db("INSERT OR IGNORE INTO recipients (user_id, name, active, org) VALUES (?, ?, 1, ?)",
       (uid, name, org))
    mark_started(uid)
    log.info("получатель подключён по приглашению #%s: user_id=%s", inv_id, uid)
    await send_text(client, uid, "Готово, вы подключены: будете получать результаты.")
    await alert_owner(f"{name} (ID {uid}) подключился(ась) по ссылке.", key=f"linked:{inv_id}", every=60.0)
    return True
