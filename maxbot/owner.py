"""Команды владельца, мастер настройки, обработка картинок по очереди."""
from __future__ import annotations

import asyncio
import json
import re
import time
import httpx
from . import base
from .ai import AIError, ai_error_text, recognize
from .base import DEFAULT_PROMPT, MSG, compose_text, fmt_ts, log, redact, split_title_body
from .max_api import ImageError, download_image
from .people import active_recipients, deliver, delivery_summary, find_recipient, invite_view, make_links, parse_names
from .storage import (archive_add, archive_rows, db, del_setting, filter_rows, get_prompt, get_setting,
                      image_hash, image_known, image_store, purge_archive, q, set_setting)
from .transport import send_text


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
                 f"Модель: {base.CFG.openrouter_model}"]
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


async def process_images(client: httpx.AsyncClient, uid: int, m: dict) -> None:
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
            archive_add(received_at, "Ошибка распознавания", "", str(path), "ошибка")
            await send_text(client, uid, ai_error_text(e))
            continue
        # заголовок записи архива — первая непустая строка ответа, тело — всё остальное
        title, body = split_title_body(result)
        aid = archive_add(received_at, title, body, str(path), "успех")
        image_store(digest, received_at, aid)
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
