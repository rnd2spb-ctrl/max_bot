"""Команды владельца, мастер настройки, обработка картинок по очереди."""
from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Optional
import httpx
from . import base
from .ai import AIError, ai_error_text, recognize
from .base import DEFAULT_PROMPT, MSG, compose_text, fmt_ts, log, redact, split_title_body
from .max_api import ImageError, download_image
from .people import (
    active_recipients, delivery_summary, dispatch_archive, find_recipient,
    invite_view, parse_names, parse_names_with_org, send_links,
)
from .storage import (
    archive_add, archive_rows, db, del_setting, filter_rows, get_prompt, get_setting,
    image_hash, image_known, image_store, purge_archive, q, recipients_for_org, set_setting,
)
from .transport import answer_callback, send_message, send_text


HELP = """/recipients — список получателей
/add [имя] [код] — создать ссылки (одно или несколько имён, каждое с новой строки)
/links — статусы ссылок
/newlink [имя] [код] — перевыпустить ссылки (одно или несколько имён, каждое с новой строки)
Код после имени: 1 — Озон, 2 — WB, 3 — обе организации (по умолчанию).
/revoke [имя] — отозвать неиспользованную ссылку; несколько имён — каждое с новой строки
/remove [имя или ID] — удалить подключённого получателя; несколько имён/ID — каждое с новой строки
/pause [имя или ID] — временно не слать получателю
/resume [имя или ID] — снова слать получателю
/archive [N] — последние N записей (по умолчанию 10)
/archive find [слово или дата] — поиск в архиве
/resend [номер] [получатель или все] [текст|картинка] — продолжить рассылку; несколько получателей — каждое с новой строки; формат обязателен, если ещё не выбран
/prompt — показать промпт
/prompt set [текст] — заменить промпт
/prompt reset — вернуть стандартный промпт
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
        rows = q("SELECT user_id, name, active, org FROM recipients ORDER BY name")
        labels = {1: "Озон", 2: "WB", 3: "Озон и WB"}
        await reply("\n".join(
            f"{n} — {u} — {'активен' if a else 'отключён'} — {labels.get(o, o)}"
            for u, n, a, o in rows
        ) or "Получателей нет.")
    elif cmd == "/add":
        names = parse_names_with_org(arg)
        if not names:
            await reply("Формат: /add Имя [код]. Код после имени: 1 — Озон, 2 — WB, 3 — обе.")
        else:
            await send_links(client, owner_id, names)
    elif cmd == "/links":
        rows = q("SELECT name, status, expires_ts, user_id FROM invites ORDER BY id DESC LIMIT 50")
        await reply("\n".join(f"{n} — {invite_view(s, e, u)}" for n, s, e, u in rows)
                    or "Ссылок ещё не создавали. Создать: /add Имя.")
    elif cmd == "/newlink":
        names = parse_names_with_org(arg)
        if not names:
            await reply("Формат: /newlink Имя [код]. Несколько имён — каждое с новой строки.")
        else:
            for name, _org in names:
                n = db("UPDATE invites SET status = 'отозвана' WHERE lower(name) = lower(?) "
                       "AND status IN ('ожидает', 'ждёт подтверждения')", (name,)).rowcount
                if n:
                    log.info("перевыпуск ссылки: %s, отозвано старых: %s", name, n)
            await send_links(client, owner_id, names)
    elif cmd == "/revoke":
        names = list(dict.fromkeys(parse_names(arg)))
        if not names:
            await reply("Формат: /revoke Имя. Несколько имён — каждое с новой строки.")
            return

        results = []
        for name in names:
            n = db(
                "UPDATE invites SET status = 'отозвана' WHERE lower(name) = lower(?) "
                "AND status IN ('ожидает', 'ждёт подтверждения')",
                (name,),
            ).rowcount
            if n:
                results.append(f"«{name}»: отозвано ссылок: {n}.")
                continue

            rec = find_recipient(name)
            if rec:
                results.append(
                    f"«{rec[1]}»: уже подключён (ID {rec[0]}); для удаления используйте /remove."
                )
            else:
                results.append(f"«{name}»: активная ссылка не найдена.")

        await reply("\n".join(results))
    elif cmd == "/remove":
        names = list(dict.fromkeys(parse_names(arg)))
        if not names:
            await reply("Формат: /remove Имя или ID. Несколько имён/ID — каждое с новой строки.")
            return

        targets = []
        seen_ids = set()
        pending_invites = []
        missing = []
        for name in names:
            rec = find_recipient(name)
            if rec:
                if rec[0] not in seen_ids:
                    targets.append(rec)
                    seen_ids.add(rec[0])
            elif q(
                "SELECT 1 FROM invites WHERE lower(name) = lower(?) "
                "AND status IN ('ожидает', 'ждёт подтверждения') LIMIT 1",
                (name,),
            ):
                pending_invites.append(name)
            else:
                missing.append(name)

        if not targets:
            messages = []
            if pending_invites:
                messages.append(
                    "Ещё не подключились (для отзыва приглашений используйте /revoke): "
                    + ", ".join(pending_invites) + "."
                )
            if missing:
                messages.append("Не найдены: " + ", ".join(missing) + ".")
            await reply("\n".join(messages) or "Подключённые получатели не найдены.")
            return

        set_setting("pending", json.dumps({
            "type": "remove",
            "targets": [list(rec) for rec in targets],
        }))
        message = "Удалить получателей: " + ", ".join(rec[1] for rec in targets) + "? Ответьте «да»."
        if pending_invites:
            message += (
                "\nЕщё не подключились (для отзыва приглашений используйте /revoke): "
                + ", ".join(pending_invites) + "."
            )
        if missing:
            message += "\nНе найдены: " + ", ".join(missing) + "."
        await reply(message)
    elif cmd == "/archive":
        rows = archive_rows()
        if arg.lower().startswith("find"):
            rows = filter_rows(rows, arg[4:])
        else:
            rows = rows[: int(arg) if arg.isdigit() else 10]
        await reply("\n".join(f"{r[0]}. {fmt_ts(r[1])} — {r[2]} [{r[4]}]" for r in rows) or "Записей нет.")
    elif cmd == "/resend":
        m = re.fullmatch(r"(\d+)\s+(.+?)(?:\s+(текст|картинка))?", arg, flags=re.IGNORECASE | re.DOTALL)
        rows = q(
            "SELECT id, ts, title, text, status, org FROM archive WHERE id = ?",
            (int(m.group(1)),),
        ) if m else []
        row = rows[0] if rows else None
        if not m or not row or row[4] != "успех":
            await reply(
                "Формат: /resend [номер] [получатель или все] [текст|картинка]. "
                "Запись должна существовать и быть успешной."
            )
            return

        recipient_arg = m.group(2).strip()
        requested_kind = (
            {"текст": "text", "картинка": "image"}.get((m.group(3) or "").lower())
        )
        dispatch_rows = q(
            "SELECT kind FROM archive_dispatch WHERE archive_id = ?",
            (row[0],),
        )
        stored_kind = dispatch_rows[0][0] if dispatch_rows else None
        if stored_kind in ("text", "image"):
            if requested_kind is not None and requested_kind != stored_kind:
                chosen = "текст" if stored_kind == "text" else "картинка"
                await reply(f"Формат уже выбран: {chosen}. Для этого материала другой формат выбрать нельзя.")
                return
            selected_kind = stored_kind
        else:
            if requested_kind is None:
                await reply(
                    f"Для этой записи формат ещё не выбран. Укажите: "
                    f"/resend {row[0]} все текст или /resend {row[0]} все картинка."
                )
                return
            selected_kind = requested_kind

        not_found = []
        paused = []
        if recipient_arg.lower() == "все":
            targets = recipients_for_org(int(row[5]))
        else:
            targets = []
            seen_ids = set()
            for requested_name in parse_names(recipient_arg):
                rec = find_recipient(requested_name)
                if not rec:
                    not_found.append(requested_name)
                    continue
                if rec[0] in seen_ids:
                    continue
                seen_ids.add(rec[0])
                if not q("SELECT 1 FROM recipients WHERE user_id = ? AND active = 1", (rec[0],)):
                    paused.append(rec[1])
                    continue
                targets.append(rec)

        if not targets:
            messages = []
            if paused:
                messages.append("На паузе (не включены в отправку): " + ", ".join(paused) + ".")
            if not_found:
                messages.append("Не найдены: " + ", ".join(not_found) + ".")
            if not messages:
                messages.append("Нет активных получателей для организации этой записи.")
            await reply("\n".join(messages))
            return
        set_setting("pending", json.dumps({
            "type": "resend",
            "archive_id": row[0],
            "kind": selected_kind,
            "targets": [list(t) for t in targets],
        }))
        kind_label = "текст" if selected_kind == "text" else "картинка"
        confirmation = (
            f"Отправить повторно №{row[0]} «{row[2]}» ({kind_label}) → "
            f"{', '.join(t[1] for t in targets)}? Ответьте «да»."
        )
        notes = []
        if paused:
            notes.append("На паузе (не включены): " + ", ".join(paused) + ".")
        if not_found:
            notes.append("Не найдены: " + ", ".join(not_found) + ".")
        if notes:
            confirmation += "\n" + "\n".join(notes)
        await reply(confirmation)
    elif cmd in ("/pause", "/resume"):
        rec = find_recipient(arg)
        if not rec:
            await reply("Получатель не найден.")
        else:
            active = 1 if cmd == "/resume" else 0
            db("UPDATE recipients SET active = ? WHERE user_id = ?", (active, rec[0]))
            await reply(f"{rec[1]}: {'включён' if active else 'отключён, рассылки не получает'}.")
    elif cmd == "/prompt":
        if arg.lower() == "reset":
            del_setting("prompt")
            await reply("Промпт сброшен на стандартный:\n\n" + get_prompt())
        elif arg.lower().startswith("set"):
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
                 f"Модель: {base.CFG.openrouter_model}"]
        await reply("\n".join(lines))
    else:
        await reply("Неизвестная команда. /help — список команд.")


async def execute_pending(client: httpx.AsyncClient, owner_id: int, p: dict) -> None:
    if p["type"] == "remove":
        targets = p.get("targets")
        if targets is None:  # совместимость с ранее сохранённым одиночным подтверждением
            user_id = p["user_id"]
            rows = q("SELECT name FROM recipients WHERE user_id = ?", (user_id,))
            name = rows[0][0] if rows else str(user_id)
            targets = [[user_id, name]]

        removed = []
        for user_id, name in targets:
            deleted = db("DELETE FROM recipients WHERE user_id = ?", (user_id,)).rowcount
            db(
                "DELETE FROM dispatch_recipients WHERE recipient_id = ? AND status != 'ok'",
                (user_id,),
            )
            if deleted:
                removed.append(name)

        if removed:
            if len(removed) == 1:
                result_text = f"Получатель «{removed[0]}» удалён."
            else:
                result_text = "Получатели удалены: " + ", ".join(removed) + "."
        else:
            result_text = "Выбранные получатели уже отсутствуют."
        await send_text(client, owner_id, result_text)
    elif p["type"] == "set_prompt":
        set_setting("prompt", p["text"])
        await send_text(client, owner_id, "Промпт заменён.")
    elif p["type"] == "resend":
        rows = q(
            "SELECT buttons_expires_ts FROM archive WHERE id = ?",
            (p["archive_id"],)
        )
        if not rows:
            await send_text(client, owner_id, "Запись уже удалена из архива.")
            return
        expires_ts = rows[0][0]
        recs = [(t[0], t[1]) for t in p["targets"]]
        result = await dispatch_archive(
            client, p["archive_id"], p["kind"], recs, expires_ts,
            allow_expired=True, extend_recipients=True,
        )
        await send_text(client, owner_id, dispatch_result_text(result, "Повторная отправка"))


def dispatch_result_text(result: dict, prefix: str = "Рассылка") -> str:
    """Понятный итог для callback и /resend."""
    state = result.get("state")
    kind = result.get("kind")
    kind_text = "текст" if kind == "text" else "картинка" if kind == "image" else "материал"
    if state == "busy":
        return "Рассылка этого материала уже выполняется. Дождитесь её завершения."
    if state == "wrong_kind":
        chosen = "текст" if kind == "text" else "картинка"
        return f"Формат уже выбран: {chosen}. Для этого материала другой формат выбрать нельзя."
    if state == "legacy_unknown":
        return ("Эта запись была разослана старой версией бота, которая не сохраняла выбранный формат. "
                "Чтобы не допустить повторную или смешанную рассылку, автоматический повтор для неё заблокирован.")
    if state == "expired":
        return "Срок действия кнопок истёк. Для повторной отправки используйте /resend."
    if state == "already_delivered":
        names = result.get("already_received", [])
        if len(names) == 1:
            return (
                f"Материал уже доставлен получателю «{names[0]}». "
                "Повторная отправка не выполнялась, чтобы не создавать дубликат."
            )
        if names:
            return (
                "Материал уже доставлен выбранным получателям: "
                + ", ".join(names)
                + ". Повторная отправка не выполнялась, чтобы не создавать дубликаты."
            )
        return "Все выбранные получатели уже получили этот материал; повторная отправка не выполнялась, чтобы не создавать дубликаты."
    if state == "completed":
        if "total" in result and int(result["total"]) == 0:
            return ("Все получатели этого материала сейчас на паузе: рассылка не выполнена. "
                    "После /resume доставьте его командой /resend <номер> <имя>.")
        return "Все выбранные получатели уже получили этот материал."
    if state == "no_recipients":
        return MSG["no_recipients"]
    if state == "no_active_recipients":
        return ("Среди недоставленных адресатов нет активных получателей, подходящих для этой организации. "
                "После включения получателя повторите /resend.")
    if state == "no_pending_targets":
        return "Для выбранного получателя нет недоставленного материала."
    if state == "missing":
        return "Материал уже удалён из архива."
    if state == "lease_lost":
        return "Не удалось продолжить рассылку: истекла блокировка. Повторите нажатие."
    if state == "upload_failed":
        return ("Не удалось загрузить изображение в MAX. В этой попытке отправка получателям не запускалась; "
                "ранее подтверждённые доставки сохранены. Повторите ту же кнопку или используйте /resend.")
    ok = int(result.get("ok", 0))
    total = int(result.get("total", 0))
    failed = result.get("failed", [])
    suffix = delivery_summary(ok, total, failed, paused=int(result.get("paused", 0)))
    return f"{prefix} ({kind_text}). {suffix}"


# =====================================================================
# 9. ПЕРВОНАЧАЛЬНАЯ НАСТРОЙКА (мастер)
# =====================================================================
def wizard_question(step: str) -> str:
    if step == "prompt":
        return ("Шаг 1 из 3: промпт для нейросети. Ответьте «да», чтобы принять стандартный "
                "(посмотреть: /prompt), или пришлите свой текст.")
    if step == "recipients":
        return ("Шаг 2 из 3: получатели. Присылайте имена (по одному или списком, каждое с новой строки). "
                "После имени можно указать код организации: 1 — Озон, 2 — WB, 3 — обе (по умолчанию). "
                "Для каждого создам персональную ссылку. Перешлите её получателю: он откроет ссылку, "
                "нажмёт Start и подтвердит своё имя. Когда закончите, напишите «готово».")
    return "Шаг 3 из 3: проверьте настройку. Ответьте «да» для завершения."


async def setup_wizard(client: httpx.AsyncClient, uid: int, text: str) -> None:
    step = get_setting("setup_step") or "start"

    async def say(s: str) -> None:
        await send_text(client, uid, s)

    if step == "start":
        del_setting("prompt_draft")                 # старый черновик из прошлой настройки не должен попасть в промпт
        set_setting("setup_step", "prompt")
        await say("Первоначальная настройка. " + wizard_question("prompt"))
    elif step == "prompt":
        if text.lower() == "да":
            set_setting("prompt", get_setting("prompt_draft") or DEFAULT_PROMPT)
            del_setting("prompt_draft")
            set_setting("setup_step", "recipients")
            await say(wizard_question("recipients"))
        else:
            set_setting("prompt_draft", text)
            await say(f"Новый промпт:\n{text}\n\nОтветьте «да», чтобы принять, или пришлите другой текст.")
    elif step == "recipients":
        if text.lower() == "готово":
            set_setting("setup_step", "confirm")
            recipient_rows = q("SELECT user_id, name, org FROM recipients WHERE active = 1 ORDER BY name")
            org_labels = {1: "Озон", 2: "WB", 3: "Озон и WB"}
            recs = "\n".join(
                f"{n} — {u} — {org_labels.get(o, o)}" for u, n, o in recipient_rows
            ) or "нет"
            await say(f"Промпт:\n{get_prompt()[:300]}\n\nПолучатели:\n{recs}\n\n" + wizard_question("confirm"))
            return
        names = parse_names_with_org(text)
        if not names:
            await say("Пришлите имя получателя или напишите «готово».")
        else:
            await send_links(client, uid, names)
            await say("Пришлите следующее имя или напишите «готово».")
    else:  # confirm
        if text.lower() == "да":
            set_setting("setup_done", "1")
            await say("Настройка завершена. Пришлите изображение. /help — список команд.")
        else:
            await say(wizard_question("confirm"))


def parse_org_directive(text: str) -> tuple[int, bool]:
    t = (text or "").strip().lower()
    mapping = {"1": 1, "2": 2, "3": 3, "озон": 1, "wb": 2, "вб": 2, "все": 3}
    if not t:
        return 3, True
    if t in mapping:
        return mapping[t], True
    return 3, False


async def process_images(client: httpx.AsyncClient, uid: int, m: dict) -> None:
    directive_text = m.get("text", "")
    target_org, recognized = parse_org_directive(directive_text)
    if directive_text.strip() and not recognized:
        await send_text(
            client, uid,
            f"Подпись «{directive_text.strip()}» не распознана как код организации, "
            "используется «обе» по умолчанию.",
        )
    purge_archive()
    base.CFG.save_dir.mkdir(parents=True, exist_ok=True)
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
        digest = image_hash(data)
        if image_known(digest):                      # та же картинка уже обрабатывалась: нейросеть не вызываем
            log.info("картинка mid=%s уже обрабатывалась (хеш %s…)", m["mid"], digest[:12])
            await send_text(client, uid, MSG["already_done"])
            continue
        path = base.CFG.save_dir / f"{int(received_at)}_{mid_part}_{i}.{fmt}"
        path.write_bytes(data)
        log.info("картинка mid=%s принята: %s, %d байт, отправляю в нейросеть", m["mid"], fmt, len(data))
        try:
            result = await recognize(client, data, fmt)
        except (AIError, httpx.HTTPError, KeyError, IndexError, ValueError) as e:
            log.error("распознавание mid=%s не удалось: %s", m["mid"], redact(str(e)))
            archive_add(received_at, "Ошибка распознавания", "", str(path), "ошибка", org=target_org)
            await send_text(client, uid, ai_error_text(e))
            continue
        # заголовок записи архива — первая непустая строка ответа, тело — всё остальное
        title, body = split_title_body(result)
        buttons_expires_ts = time.time() + base.CFG.button_ttl_hours * 3600
        aid = archive_add(
            received_at, title, body, str(path), "успех",
            buttons_expires_ts=buttons_expires_ts,
            org=target_org,
        )
        image_store(digest, received_at, aid)
        log.info("распознано: запись архива №%d, заголовок «%s», тело %d знаков", aid, title[:60], len(body))
        org_label = {1: "Озон", 2: "WB", 3: "Озон и WB"}[target_org]
        if not recipients_for_org(target_org):
            await send_text(client, uid, f"Нет активных получателей для организации «{org_label}».")
            continue
        await send_text(client, uid, compose_text(title, body))
        buttons = [[
            {"type": "callback", "text": "Текст", "payload": f"deliver:text:{aid}"},
            {"type": "callback", "text": "Картинка", "payload": f"deliver:image:{aid}"},
        ]]
        await send_message(
            client, uid,
            f"Что разослать получателям ({org_label}) — распознанный текст или саму картинку?",
            attachments=[{"type": "inline_keyboard", "payload": {"buttons": buttons}}],
        )


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
# 15. НАЖАТИЕ INLINE-КНОПКИ («Текст» / «Картинка» после распознавания)
# =====================================================================
async def handle_callback(client: httpx.AsyncClient, update: dict) -> None:
    """Обрабатывает выбор формата; формат закрепляется за записью навсегда."""
    cb = update.get("callback") if isinstance(update.get("callback"), dict) else update
    callback_id = cb.get("callback_id") or update.get("callback_id")
    payload = cb.get("payload") if cb.get("payload") is not None else update.get("payload")
    user = cb.get("user") if isinstance(cb.get("user"), dict) else (update.get("user") or {})
    uid = user.get("user_id")

    async def ans(
        text: Optional[str] = None,
        attachments: Optional[list[dict]] = None,
        remove_attachments: bool = True,
    ) -> None:
        if callback_id:
            await answer_callback(
                client, callback_id, text,
                attachments=attachments,
                remove_attachments=remove_attachments,
            )
        elif text is not None and uid is not None:
            await send_text(client, uid, text)

    if uid != base.CFG.owner_id:
        log.info("message_callback от не-владельца user_id=%s, игнорирую", uid)
        return
    m = re.fullmatch(r"deliver:(text|image):(\d+)", (payload or "").strip())
    if not m:
        log.warning("message_callback: неизвестный или устаревший payload %r", payload)
        await ans("Кнопка устарела.")
        return

    kind, aid = m.group(1), int(m.group(2))
    rows = q(
        "SELECT ts, title, text, image_path, buttons_expires_ts, status, org "
        "FROM archive WHERE id = ?",
        (aid,)
    )
    if not rows:
        await ans("Запись архива не найдена (возможно, уже удалена по сроку хранения).")
        return
    ts, title, body, image_path, expires_ts, archive_status, org = rows[0]
    if archive_status != "успех":
        await ans("Для этой записи рассылка недоступна: распознавание завершилось ошибкой.")
        return
    if expires_ts is None:
        expires_ts = ts + base.CFG.button_ttl_hours * 3600
    if expires_ts <= time.time():
        await ans("Срок действия кнопок истёк. Для повторной отправки используйте /resend.")
        return

    recs = recipients_for_org(int(org))
    result = await dispatch_archive(client, aid, kind, recs, expires_ts)
    log.info("рассылка записи №%d: выбранный формат=%s, состояние=%s, доставлено=%s/%s",
             aid, result.get("kind", kind), result.get("state"),
             result.get("ok", 0), result.get("total", 0))
    result_state = result.get("state")
    selected_kind = result.get("kind")
    if result_state in ("partial", "upload_failed", "wrong_kind", "lease_lost"):
        if expires_ts <= time.time():
            await ans(
                "Рассылка осталась незавершённой, но срок действия кнопок истёк. "
                "Продолжить её можно командой /resend.",
                remove_attachments=True,
            )
            return
        # Пока есть недоставленные получатели и кнопки ещё действуют,
        # оставляем только кнопку выбранного формата.
        button = None
        if selected_kind in ("text", "image"):
            button_text = "Текст" if selected_kind == "text" else "Картинка"
            button = [[{
                "type": "callback",
                "text": button_text,
                "payload": f"deliver:{selected_kind}:{aid}",
            }]]
        attachments = (
            [{"type": "inline_keyboard", "payload": {"buttons": button}}]
            if button else None
        )
        await ans(
            dispatch_result_text(result),
            attachments=attachments,
            remove_attachments=False,
        )
    elif result_state in ("busy", "no_recipients"):
        # Не пересоздаём клавиатуру: другая задача может ещё выполнять рассылку.
        await ans(dispatch_result_text(result), remove_attachments=False)
    else:
        # Полная доставка, истёкшая кнопка или несуществующая запись: убираем клавиатуру.
        await ans(dispatch_result_text(result), remove_attachments=True)
