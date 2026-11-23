"""SQLite: настройки, очередь, архив, входящие, отметки."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
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
        db("UPDATE inbox SET done = 1 WHERE id = ?", (inbox_id,))
    else:
        db("UPDATE inbox SET done = 1, payload = ? WHERE id = ?", (payload, inbox_id))


def inbox_pending() -> list[tuple[int, dict]]:
    out = []
    for iid, payload in q("SELECT id, payload FROM inbox WHERE done = 0 ORDER BY id"):
        try:
            out.append((iid, json.loads(payload)))
        except ValueError:
            log.error("inbox id=%s: повреждённая запись, пропущена", iid)
            inbox_done(iid)
    return out


def archive_add(ts: float, title: str, text: str, image_path: str, status: str) -> int:
    return int(db("INSERT INTO archive (ts, title, text, image_path, status) VALUES (?, ?, ?, ?, ?)",
                  (ts, title, text, image_path, status)).lastrowid)


# ---------- картинки: сравнение по хешу SHA-256 ----------
def image_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def image_known(digest: str) -> bool:
    return bool(q("SELECT 1 FROM images WHERE hash = ?", (digest,)))


def image_store(digest: str, ts: float, aid: int) -> None:
    db("INSERT OR IGNORE INTO images (hash, ts, archive_id) VALUES (?, ?, ?)", (digest, ts, aid))


def archive_delivered(archive_id: int) -> bool:
    """True, если по записи архива уже была хоть одна попытка рассылки (успешная или нет).
    Защита от повторной рассылки при двойном нажатии кнопки или нажатии на старую кнопку
    после перезапуска бота."""
    return bool(q("SELECT 1 FROM deliveries WHERE archive_id = ? LIMIT 1", (archive_id,)))


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
        db("DELETE FROM archive WHERE id = ?", (aid,))
    db("DELETE FROM inbox WHERE done = 1 AND ts < ?", (now - 86400,))
    db("DELETE FROM seen WHERE ts < ?", (cutoff,))
    db("DELETE FROM images WHERE ts < ?", (cutoff,))
