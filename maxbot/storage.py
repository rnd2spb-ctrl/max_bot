"""SQLite: настройки, очередь, архив, входящие, отметки."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional
from . import base
from .base import DEFAULT_PROMPT, log, parse_date


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
    user_id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1,
    org INTEGER NOT NULL DEFAULT 3);
CREATE TABLE IF NOT EXISTS archive (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, title TEXT,
    text TEXT, image_path TEXT, status TEXT NOT NULL,
    org INTEGER NOT NULL DEFAULT 3);
CREATE TABLE IF NOT EXISTS deliveries (archive_id INTEGER, recipient_id INTEGER, status TEXT, ts REAL);
CREATE TABLE IF NOT EXISTS archive_dispatch (
    archive_id INTEGER PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
    created_ts REAL NOT NULL, updated_ts REAL NOT NULL,
    expires_ts REAL, lease_until REAL, lease_token TEXT, last_error TEXT);
CREATE TABLE IF NOT EXISTS dispatch_recipients (
    archive_id INTEGER NOT NULL, recipient_id INTEGER NOT NULL,
    recipient_name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0, updated_ts REAL NOT NULL, last_error TEXT,
    PRIMARY KEY (archive_id, recipient_id));
CREATE TABLE IF NOT EXISTS request_log (ts REAL, user_id INTEGER, text TEXT);
CREATE TABLE IF NOT EXISTS inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, payload TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_ts REAL NOT NULL DEFAULT 0, last_error TEXT);
CREATE TABLE IF NOT EXISTS invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, token_hash TEXT NOT NULL UNIQUE,
    created_ts REAL NOT NULL, expires_ts REAL NOT NULL,
    status TEXT NOT NULL,              -- ожидает | ждёт подтверждения | подключён | отозвана | истекла
    user_id INTEGER, used_ts REAL, org INTEGER NOT NULL DEFAULT 3);
CREATE TABLE IF NOT EXISTS link_attempts (user_id INTEGER NOT NULL, ts REAL NOT NULL);
CREATE TABLE IF NOT EXISTS images (
    hash TEXT PRIMARY KEY, ts REAL NOT NULL, archive_id INTEGER);
"""


def _lower_unicode(value: Any) -> Any:
    return value.lower() if isinstance(value, str) else value


def init_db(path: Optional[str] = None) -> None:
    """Открывает (создаёт) базу. Вызывается один раз при старте."""
    global _db
    if _db is not None:
        _db.close()
    _db = sqlite3.connect(path or base.CFG.db_path, check_same_thread=False)
    # Встроенный lower() в SQLite понимает только латиницу: «анна» не находила «Анна».
    # Подменяем его версией Python, которая работает с кириллицей и любым Unicode.
    _db.create_function("lower", 1, _lower_unicode, deterministic=True)
    _db.execute("PRAGMA busy_timeout = 30000")
    _db.executescript(SCHEMA)

    # Миграции существующей базы: добавляем столбцы без удаления старых данных.
    migrations = (
        ("archive", "buttons_expires_ts", "REAL"),
        ("inbox", "attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("inbox", "next_attempt_ts", "REAL NOT NULL DEFAULT 0"),
        ("inbox", "last_error", "TEXT"),
        ("archive_dispatch", "lease_token", "TEXT"),
    )
    for table, column, definition in migrations:
        columns = {row[1] for row in _db.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            _db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
    _db.execute("CREATE INDEX IF NOT EXISTS idx_dispatch_recipients_status "
                "ON dispatch_recipients(archive_id, status)")

    # Старая таблица deliveries не сохраняла выбранный формат рассылки.
    # Для таких архивов нельзя безопасно угадывать формат и повторно отправлять материал.
    _db.execute(
        "INSERT OR IGNORE INTO archive_dispatch "
        "(archive_id, kind, status, created_ts, updated_ts, expires_ts) "
        "SELECT a.id, 'legacy_unknown', 'partial', MIN(d.ts), MAX(d.ts), "
        "a.ts + ? FROM archive a JOIN deliveries d ON d.archive_id = a.id "
        "WHERE NOT EXISTS (SELECT 1 FROM archive_dispatch x WHERE x.archive_id = a.id) "
        "GROUP BY a.id",
        (base.CFG.button_ttl_hours * 3600,)
    )
    _db.execute(
        "INSERT OR IGNORE INTO dispatch_recipients "
        "(archive_id, recipient_id, recipient_name, status, attempts, updated_ts, last_error) "
        "SELECT d.archive_id, d.recipient_id, "
        "COALESCE((SELECT r.name FROM recipients r WHERE r.user_id = d.recipient_id), "
        "'ID ' || d.recipient_id), "
        "CASE WHEN MAX(CASE WHEN d.status = 'ok' THEN 1 ELSE 0 END) = 1 "
        "THEN 'ok' ELSE 'fail' END, COUNT(*), MAX(d.ts), "
        "'Старая версия не сохраняла выбранный формат рассылки' "
        "FROM deliveries d JOIN archive_dispatch a ON a.archive_id = d.archive_id "
        "WHERE a.kind = 'legacy_unknown' "
        "GROUP BY d.archive_id, d.recipient_id"
    )
    _db.commit()

    # После перезапуска предыдущий процесс уже не выполняет рассылку:
    # освобождаем его незавершённые аренды, сохраняя отметки успешной доставки.
    now = time.time()
    _db.execute(
        "UPDATE dispatch_recipients SET status = 'pending', updated_ts = ?, "
        "last_error = COALESCE(last_error, 'Процесс остановился до подтверждения результата отправки') "
        "WHERE status = 'sending'",
        (now,)
    )
    _db.execute(
        "UPDATE archive_dispatch SET status = 'partial', lease_until = NULL, "
        "lease_token = NULL, updated_ts = ? WHERE status = 'sending'",
        (now,)
    )
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


def get_prompt() -> str:
    return get_setting("prompt") or DEFAULT_PROMPT


# ---------- идемпотентность ----------
def claim_mid(mid: str) -> bool:
    """True, если сообщение с таким mid видим впервые (и помечаем его обработанным)."""
    return db("INSERT OR IGNORE INTO seen (mid, ts) VALUES (?, ?)", (mid, time.time())).rowcount == 1


def release_mid(mid: str) -> None:
    db("DELETE FROM seen WHERE mid = ?", (mid,))


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
    """Помечает обновление обработанным. Токен ссылки-приглашения (поле payload у bot_started)
    после обработки затирается, чтобы он не лежал в базе открытым текстом."""
    payload = None
    rows = q("SELECT payload FROM inbox WHERE id = ?", (inbox_id,))
    if rows:
        try:
            u = json.loads(rows[0][0])
            if isinstance(u, dict) and u.get("payload"):
                u["payload"] = "***"
                payload = json.dumps(u, ensure_ascii=False)
        except ValueError:
            pass
    if payload is None:
        db("UPDATE inbox SET done = 1, next_attempt_ts = 0 WHERE id = ?", (inbox_id,))
    else:
        db("UPDATE inbox SET done = 1, payload = ?, next_attempt_ts = 0 WHERE id = ?",
           (payload, inbox_id))


def inbox_pending() -> list[tuple[int, dict, float]]:
    out = []
    now = time.time()
    for iid, payload, next_attempt_ts in q(
        "SELECT id, payload, next_attempt_ts FROM inbox WHERE done = 0 ORDER BY id"
    ):
        try:
            out.append((iid, json.loads(payload), max(0.0, (next_attempt_ts or 0.0) - now)))
        except ValueError:
            log.error("inbox id=%s: повреждённая запись, пропущена", iid)
            inbox_done(iid)
    return out


def inbox_retry(inbox_id: int, error: str, max_attempts: int = 5) -> tuple[int, float, bool]:
    """Фиксирует неудачную обработку события и рассчитывает задержку повтора.

    Возвращает (номер попытки, задержка, исчерпан ли лимит).
    """
    now = time.time()
    c = _conn()
    with c:
        row = c.execute(
            "SELECT attempts FROM inbox WHERE id = ? AND done = 0", (inbox_id,)
        ).fetchone()
        if row is None:
            return 0, 0.0, True
        attempts = int(row[0] or 0) + 1
        if attempts >= max_attempts:
            c.execute(
                "UPDATE inbox SET attempts = ?, done = 1, last_error = ?, next_attempt_ts = 0 "
                "WHERE id = ?",
                (attempts, error[:1000], inbox_id)
            )
            return attempts, 0.0, True
        delay = min(60.0, 2.0 ** (attempts - 1))
        c.execute(
            "UPDATE inbox SET attempts = ?, last_error = ?, next_attempt_ts = ? WHERE id = ?",
            (attempts, error[:1000], now + delay, inbox_id)
        )
        return attempts, delay, False


def recipients_for_org(target_org: int) -> list[tuple[int, str]]:
    """Активные получатели, чья организация пересекается с целевой маской."""
    return q(
        "SELECT user_id, name FROM recipients "
        "WHERE active = 1 AND (org & ?) != 0 ORDER BY name",
        (target_org,),
    )


def active_recipient_ids(ids: list[int]) -> set[int]:
    """Из переданных ID возвращает тех, кто сейчас существует и active=1."""
    unique_ids = list(dict.fromkeys(int(uid) for uid in ids))
    if not unique_ids:
        return set()
    placeholders = ",".join("?" for _ in unique_ids)
    rows = q(
        f"SELECT user_id FROM recipients WHERE active = 1 AND user_id IN ({placeholders})",
        unique_ids,
    )
    return {int(row[0]) for row in rows}


def archive_add(
    ts: float, title: str, text: str, image_path: str, status: str,
    buttons_expires_ts: Optional[float] = None, org: int = 3,
) -> int:
    return int(db(
        "INSERT INTO archive (ts, title, text, image_path, status, buttons_expires_ts, org) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (ts, title, text, image_path, status, buttons_expires_ts, org)
    ).lastrowid)


# ---------- состояние и блокировка рассылки одного материала ----------
DISPATCH_LEASE_SECONDS = 180.0


def claim_archive_dispatch(
    archive_id: int,
    kind: Optional[str],
    recipients: list[tuple[int, str]],
    expires_ts: Optional[float],
    *,
    allow_expired: bool = False,
    extend_recipients: bool = False,
    default_kind: str = "text",
    now: Optional[float] = None,
) -> dict:
    """Атомарно закрепляет формат и получает аренду на рассылку одного материала."""
    now = time.time() if now is None else now
    c = _conn()
    token = uuid.uuid4().hex
    c.execute("BEGIN IMMEDIATE")
    try:
        row = c.execute(
            "SELECT kind, status, expires_ts, lease_until "
            "FROM archive_dispatch WHERE archive_id = ?",
            (archive_id,)
        ).fetchone()

        if row is None:
            if not recipients:
                c.commit()
                return {"state": "no_recipients"}
            if not allow_expired and (expires_ts is None or expires_ts <= now):
                c.commit()
                return {"state": "expired"}
            selected_kind = kind or default_kind
            if selected_kind not in ("text", "image"):
                raise ValueError(f"неизвестный формат рассылки: {selected_kind}")
            c.execute(
                "INSERT INTO archive_dispatch "
                "(archive_id, kind, status, created_ts, updated_ts, expires_ts, lease_until, lease_token) "
                "VALUES (?, ?, 'sending', ?, ?, ?, ?, ?)",
                (archive_id, selected_kind, now, now, expires_ts,
                 now + DISPATCH_LEASE_SECONDS, token)
            )
            c.executemany(
                "INSERT OR IGNORE INTO dispatch_recipients "
                "(archive_id, recipient_id, recipient_name, status, updated_ts) "
                "VALUES (?, ?, ?, 'pending', ?)",
                [(archive_id, uid, name, now) for uid, name in recipients]
            )
        else:
            selected_kind, status, stored_expiry, lease_until = row
            if selected_kind not in ("text", "image"):
                c.commit()
                return {"state": "legacy_unknown", "kind": selected_kind}
            if kind is not None and kind != selected_kind:
                c.commit()
                return {"state": "wrong_kind", "kind": selected_kind}
            if not allow_expired and (stored_expiry is None or stored_expiry <= now):
                c.commit()
                return {"state": "expired", "kind": selected_kind}
            if status == "completed" and not extend_recipients:
                c.commit()
                return {"state": "completed", "kind": selected_kind}
            if lease_until is not None and lease_until > now:
                c.commit()
                return {"state": "busy", "kind": selected_kind}

            if extend_recipients:
                c.executemany(
                    "INSERT OR IGNORE INTO dispatch_recipients "
                    "(archive_id, recipient_id, recipient_name, status, updated_ts) "
                    "VALUES (?, ?, ?, 'pending', ?)",
                    [(archive_id, uid, name, now) for uid, name in recipients]
                )

            if extend_recipients:
                target_ids = list(dict.fromkeys(uid for uid, _ in recipients))
                if not target_ids:
                    c.commit()
                    return {"state": "no_recipients", "kind": selected_kind}
                placeholders = ",".join("?" for _ in target_ids)
                pending = c.execute(
                    "SELECT recipient_id FROM dispatch_recipients "
                    f"WHERE archive_id = ? AND status != 'ok' AND recipient_id IN ({placeholders}) LIMIT 1",
                    [archive_id, *target_ids]
                ).fetchone()
            else:
                pending = c.execute(
                    "SELECT recipient_id FROM dispatch_recipients "
                    "WHERE archive_id = ? AND status != 'ok' LIMIT 1",
                    (archive_id,)
                ).fetchone()

            if pending is None:
                total, ok = c.execute(
                    "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END), 0) "
                    "FROM dispatch_recipients WHERE archive_id = ?",
                    (archive_id,)
                ).fetchone()
                if total and ok == total:
                    c.execute(
                        "UPDATE archive_dispatch SET status = 'completed', lease_until = NULL, "
                        "lease_token = NULL, updated_ts = ? WHERE archive_id = ?",
                        (now, archive_id)
                    )
                    c.commit()
                    return {"state": "completed", "kind": selected_kind}
                c.commit()
                return {"state": "no_pending_targets", "kind": selected_kind}

            c.execute(
                "UPDATE archive_dispatch SET status = 'sending', lease_until = ?, lease_token = ?, "
                "updated_ts = ?, last_error = NULL WHERE archive_id = ?",
                (now + DISPATCH_LEASE_SECONDS, token, now, archive_id)
            )

        if extend_recipients:
            target_ids = list(dict.fromkeys(uid for uid, _ in recipients))
            placeholders = ",".join("?" for _ in target_ids)
            pending = c.execute(
                "SELECT recipient_id, recipient_name, status FROM dispatch_recipients "
                f"WHERE archive_id = ? AND status != 'ok' AND recipient_id IN ({placeholders}) "
                "ORDER BY rowid",
                [archive_id, *target_ids]
            ).fetchall()
        else:
            pending = c.execute(
                "SELECT recipient_id, recipient_name, status FROM dispatch_recipients "
                "WHERE archive_id = ? AND status != 'ok' ORDER BY rowid",
                (archive_id,)
            ).fetchall()
        c.commit()
        return {
            "state": "claimed",
            "kind": selected_kind,
            "lease_token": token,
            "recipients": [(int(uid), name, status) for uid, name, status in pending],
        }
    except BaseException:
        if c.in_transaction:
            c.rollback()
        raise


def dispatch_recipients(archive_id: int) -> list[tuple[int, str, str]]:
    """Возвращает по каждому адресату номер, имя и состояние доставки."""
    return [
        (int(uid), name, status)
        for uid, name, status in q(
            "SELECT recipient_id, recipient_name, status FROM dispatch_recipients "
            "WHERE archive_id = ? ORDER BY rowid",
            (archive_id,)
        )
    ]


def mark_dispatch_recipient_sending(
    archive_id: int, recipient_id: int, lease_token: str,
) -> bool:
    now = time.time()
    c = _conn()
    with c:
        cur = c.execute(
            "UPDATE dispatch_recipients SET status = 'sending', updated_ts = ?, last_error = NULL "
            "WHERE archive_id = ? AND recipient_id = ? AND status != 'ok' "
            "AND EXISTS (SELECT 1 FROM recipients r "
            "WHERE r.user_id = dispatch_recipients.recipient_id AND r.active = 1) "
            "AND EXISTS (SELECT 1 FROM archive_dispatch WHERE archive_id = ? "
            "AND status = 'sending' AND lease_token = ? AND lease_until > ?)",
            (now, archive_id, recipient_id, archive_id, lease_token, now)
        )
    return cur.rowcount == 1


def touch_archive_dispatch(archive_id: int, lease_token: str) -> bool:
    now = time.time()
    cur = db(
        "UPDATE archive_dispatch SET lease_until = ?, updated_ts = ? "
        "WHERE archive_id = ? AND status = 'sending' AND lease_token = ?",
        (now + DISPATCH_LEASE_SECONDS, now, archive_id, lease_token)
    )
    return cur.rowcount == 1


def record_dispatch_delivery(
    archive_id: int, recipient_id: int, lease_token: str,
    status: str, error: Optional[str] = None,
) -> None:
    if status not in ("ok", "fail"):
        raise ValueError(f"неизвестный статус доставки: {status}")
    now = time.time()
    c = _conn()
    with c:
        lease = c.execute(
            "SELECT 1 FROM archive_dispatch WHERE archive_id = ? "
            "AND status = 'sending' AND lease_token = ?",
            (archive_id, lease_token)
        ).fetchone()
        if lease is None:
            raise RuntimeError("аренда рассылки потеряна до записи результата")
        cur = c.execute(
            "UPDATE dispatch_recipients SET status = ?, attempts = attempts + 1, "
            "updated_ts = ?, last_error = ? WHERE archive_id = ? AND recipient_id = ? "
            "AND status != 'ok'",
            (status, now, error, archive_id, recipient_id)
        )
        if cur.rowcount != 1:
            raise RuntimeError("состояние получателя рассылки изменилось неожиданно")
        c.execute(
            "INSERT INTO deliveries (archive_id, recipient_id, status, ts) VALUES (?, ?, ?, ?)",
            (archive_id, recipient_id, status, now)
        )


def finish_archive_dispatch(
    archive_id: int, lease_token: str, error: Optional[str] = None,
) -> dict:
    now = time.time()
    c = _conn()
    with c:
        lease = c.execute(
            "SELECT 1 FROM archive_dispatch WHERE archive_id = ? "
            "AND status = 'sending' AND lease_token = ?",
            (archive_id, lease_token)
        ).fetchone()
        if lease is None:
            raise RuntimeError("аренда рассылки потеряна до завершения")
        # Если исключение прервало отправку после начала сетевого запроса,
        # результат считается неподтверждённым и попадёт в список для повтора.
        c.execute(
            "UPDATE dispatch_recipients SET status = 'fail', attempts = attempts + 1, "
            "updated_ts = ?, last_error = COALESCE(?, 'Результат отправки не подтверждён') "
            "WHERE archive_id = ? AND status = 'sending'",
            (now, error, archive_id)
        )
        total, ok = c.execute(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END), 0) "
            "FROM dispatch_recipients WHERE archive_id = ?",
            (archive_id,)
        ).fetchone()
        failed = c.execute(
            "SELECT recipient_name FROM dispatch_recipients "
            "WHERE archive_id = ? AND status != 'ok' ORDER BY rowid",
            (archive_id,)
        ).fetchall()
        state = "completed" if total and ok == total else "partial"
        c.execute(
            "UPDATE archive_dispatch SET status = ?, lease_until = NULL, lease_token = NULL, "
            "updated_ts = ?, last_error = ? WHERE archive_id = ? AND lease_token = ?",
            (state, now, error, archive_id, lease_token)
        )
    return {
        "state": state,
        "total": int(total),
        "ok": int(ok),
        "failed": [row[0] for row in failed],
        "error": error,
    }


def archive_dispatch_info(archive_id: int) -> Optional[tuple]:
    rows = q(
        "SELECT kind, status, expires_ts, lease_until, last_error "
        "FROM archive_dispatch WHERE archive_id = ?",
        (archive_id,)
    )
    return rows[0] if rows else None


# ---------- картинки: сравнение по хешу SHA-256 ----------
def image_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def image_known(digest: str) -> bool:
    return bool(q("SELECT 1 FROM images WHERE hash = ?", (digest,)))


def image_store(digest: str, ts: float, aid: int) -> None:
    db("INSERT OR IGNORE INTO images (hash, ts, archive_id) VALUES (?, ?, ?)", (digest, ts, aid))


def archive_delivered(archive_id: int) -> bool:
    """True, только когда все выбранные получатели получили материал успешно."""
    return bool(q(
        "SELECT 1 FROM archive_dispatch WHERE archive_id = ? AND status = 'completed'",
        (archive_id,)
    ))


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
    cutoff = now - base.CFG.archive_days * 86400
    for aid, path in q("SELECT id, image_path FROM archive WHERE ts < ?", (cutoff,)):
        if path:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError as e:
                log.warning("не удалось удалить файл %s: %r", path, e)
        db("DELETE FROM deliveries WHERE archive_id = ?", (aid,))
        db("DELETE FROM dispatch_recipients WHERE archive_id = ?", (aid,))
        db("DELETE FROM archive_dispatch WHERE archive_id = ?", (aid,))
        db("DELETE FROM archive WHERE id = ?", (aid,))
    db("DELETE FROM inbox WHERE done = 1 AND ts < ?", (now - 86400,))
    db("DELETE FROM seen WHERE ts < ?", (cutoff,))
    db("DELETE FROM images WHERE ts < ?", (cutoff,))
