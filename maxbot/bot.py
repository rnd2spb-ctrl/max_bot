"""Главный цикл: опрос, фоновые задачи, запуск и остановка."""
from __future__ import annotations

import asyncio
import logging
import random
import signal
import sys
import time
from typing import Awaitable, Optional
import httpx
from . import base, transport
from .ai import check_model_vision
from .base import BACKOFF_MAX, ConfigError, MSG, SHUTDOWN_GRACE, load_config, log, redact
from .max_api import AuthError, TransientApiError, check_webhook, display_name, get_updates, parse_message
from .owner import handle_callback, handle_owner
from .people import active_recipients, find_recipient, first_time_stranger, handle_invite_reply, handle_invite_start, handle_recipient_request, has_started, is_recipient, mark_started
from .storage import claim_mid, inbox_done, inbox_pending, inbox_retry, inbox_store, init_db, load_marker, purge_archive, release_mid
from .transport import alert_owner, send_text


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
        if uid is not None and payload and uid != base.CFG.owner_id:
            await handle_invite_start(client, uid, payload, display_name(update.get("user")))
            return
        if uid is not None:
            first = not has_started(uid)
            mark_started(uid)
            if uid != base.CFG.owner_id:
                rec = find_recipient(str(uid))
                if rec and first:
                    await alert_owner(f"{rec[1]} (ID {uid}) нажал(а) Start: теперь получает рассылку.",
                                      key=f"start:{uid}", every=3600.0)
                elif not rec:
                    await alert_owner(stranger_text(uid, display_name(update.get("user"))),
                                      key=f"stranger:{uid}", every=86400.0)
        return
    if update.get("update_type") == "message_callback":
        await handle_callback(client, update)
        return
    m = parse_message(update)
    if not m or not m["mid"] or m["sender_id"] is None:
        return
    mid, uid = m["mid"], m["sender_id"]

    if uid == base.CFG.owner_id:
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


_tasks: set[asyncio.Task] = set()


def spawn(
    coro: Awaitable[None],
    inbox_id: Optional[int] = None,
    *,
    retry_update: Optional[dict] = None,
    retry_client: Optional[httpx.AsyncClient] = None,
    initial_delay: float = 0.0,
) -> asyncio.Task:
    """Фоновая задача обработки события.

    Для callback используются ограниченные повторы с сохранённой задержкой.
    Остальные типы событий сохраняют прежнюю политику: исключение журналируется,
    владельцу отправляется уведомление, событие помечается выполненным.
    """
    async def runner() -> None:
        current = coro
        try:
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)
        except asyncio.CancelledError:
            close = getattr(current, "close", None)
            if close:
                close()
            raise

        while True:
            try:
                await current
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if (inbox_id is not None and retry_update is not None
                        and retry_client is not None):
                    attempts, delay, exhausted = inbox_retry(
                        inbox_id, f"{type(e).__name__}: {e}"
                    )
                    if not exhausted:
                        log.exception(
                            "обработка callback не удалась, повтор №%d через %.1f с (inbox_id=%s)",
                            attempts, delay, inbox_id
                        )
                        await asyncio.sleep(delay)
                        current = process(retry_update, retry_client, replayed=True)
                        continue
                    log.exception(
                        "callback не обработан после %d попыток (inbox_id=%s)",
                        attempts, inbox_id
                    )
                    await alert_owner(
                        "Не удалось обработать нажатие кнопки после нескольких попыток. "
                        "Состояние рассылки сохранено; нажмите ту же кнопку ещё раз или используйте /resend.",
                        key=f"callback_err:{inbox_id}", every=300.0
                    )
                    inbox_done(inbox_id)
                    return

                log.exception("обновление отброшено из-за необработанной ошибки (inbox_id=%s)", inbox_id)
                await alert_owner(
                    "Необработанная ошибка при обработке события, событие отброшено. Подробности в журнале.",
                    key="task_err", every=300.0
                )
                if inbox_id is not None:
                    inbox_done(inbox_id)
                return
            else:
                if inbox_id is not None:
                    inbox_done(inbox_id)
                return
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


def replay_inbox(client: httpx.AsyncClient) -> int:
    """При старте заново запускает обновления, сохранённые, но не обработанные
    (падение или остановка посреди обработки). Дубли отсекаются по mid."""
    pend = inbox_pending()
    for iid, upd, delay in pend:
        is_callback = upd.get("update_type") == "message_callback"
        spawn(
            process(upd, client, replayed=True), iid,
            retry_update=upd if is_callback else None,
            retry_client=client if is_callback else None,
            initial_delay=delay,
        )
    if pend:
        log.warning("восстановлено необработанных обновлений: %d", len(pend))
    return len(pend)


async def poll_loop(client: httpx.AsyncClient, max_polls: Optional[int] = None) -> None:
    """Бесконечный опрос. max_polls нужен только тестам.

    Порядок защиты от пропуска событий: получили пачку -> в одной транзакции записали
    её в inbox и сдвинули marker -> запустили обработку. Если процесс упадёт после этого,
    события возьмутся из inbox; если до — marker не сдвинут, MAX отдаст их снова."""
    marker = load_marker()
    log.info("long polling запущен, marker=%s, timeout=%s, limit=%s", marker, base.CFG.poll_timeout, base.CFG.poll_limit)
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
            await transport._sleep(wait)
            continue
        except (httpx.HTTPError, ValueError) as e:   # сеть, таймаут, битый JSON
            wait = backoff_delay(fails)
            fails += 1
            log.warning("опрос: %r, пауза %.1f с (подряд ошибок: %d)", e, wait, fails)
            if fails >= 5:
                await alert_owner(f"Опрос MAX: {fails} ошибок подряд, последняя: {e!r}. Бот продолжает попытки.",
                                  key="poll_fail", every=3600.0)
            await transport._sleep(wait)
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
            is_callback = upd.get("update_type") == "message_callback"
            spawn(
                process(upd, client), iid,
                retry_update=upd if is_callback else None,
                retry_client=client if is_callback else None,
            )
        if updates:
            log.debug("получено обновлений: %d, marker=%s", len(updates), marker)
        elif time.monotonic() - started < 0.2:       # сервер ответил мгновенно и пусто: не крутим CPU
            await transport._sleep(1.0)

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
# 14. ЗАПУСК
# =====================================================================
async def amain() -> None:
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
        transport._http = client
        try:
            await check_webhook(client)
            await check_model_vision(client)
            restored = replay_inbox(client)
            await alert_owner(f"Бот запущен. Модель: {base.CFG.openrouter_model}. "
                              f"Получателей: {len(active_recipients())}. Восстановлено событий: {restored}.",
                              force=True)
            await poll_loop(client)
        finally:
            await drain_tasks()
            transport._http = None
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
