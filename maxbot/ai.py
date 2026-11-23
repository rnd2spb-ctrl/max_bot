"""Клиент OpenRouter: запрос, повтор при 429 (2, 3, 5, 8 с), ошибки нейросети."""
from __future__ import annotations

import asyncio
import base64
from typing import Any, Optional
import httpx
from . import base, transport
from .base import AI_RETRY_PAUSES, AI_TIMEOUT, MSG, OPENROUTER_MODELS_URL, OPENROUTER_URL, log, redact
from .storage import get_prompt
from .transport import alert_owner


# =====================================================================
# 6. КЛИЕНТ НЕЙРОСЕТИ (OpenRouter)
# =====================================================================
class AIError(Exception):
    """Распознавание не удалось (после повторов или без смысла повторять).

    kind: limit (лимит исчерпан), credits (нет кредитов), auth (ключ или доступ),
    model (модель не найдена), other."""

    def __init__(self, msg: str, kind: str = "other"):
        super().__init__(msg)
        self.kind = kind


def build_ai_payload(prompt: str, data: bytes, fmt: str) -> dict:
    """Тело запроса /chat/completions: текст первым, затем картинка как data-URL (base64).

    max_tokens задан явно (иначе провайдер может подставить маленький лимит по умолчанию).
    reasoning.enabled=False: у моделей с режимом «рассуждений» без этого флага весь лимит
    токенов может уйти на скрытые рассуждения, а видимый ответ придёт пустым или обрезанным."""
    mime = {"jpg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[fmt]
    b64 = base64.b64encode(data).decode()
    return {
        "model": base.CFG.openrouter_model,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ],
        }],
        "max_tokens": base.CFG.openrouter_max_tokens,
        "reasoning": {"enabled": False},
        "stream": False,
    }


def ai_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {base.CFG.openrouter_api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": base.CFG.openrouter_referer,
        "X-Title": base.CFG.openrouter_title,
    }


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except ValueError:
        return None


def _error_text(body: Any, r: httpx.Response) -> str:
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        e = body["error"]
        return f"{e.get('code', '')} {e.get('message', '')}".strip()[:300]
    return (r.text or "")[:300]


_ai_sem: Optional[asyncio.Semaphore] = None   # создаётся при первом запросе


async def recognize(client: httpx.AsyncClient, data: bytes, fmt: str) -> str:
    """Отправляет картинку в OpenRouter и возвращает текст ответа.

    Запросы идут строго по одному (семафор _ai_sem): картинки распознаются в порядке поступления.
    Повторы после ошибок 429, 408, 5xx, сети, таймаута и пустого ответа: паузы AI_RETRY_PAUSES
    (2, 3, 5, 8 с), всего пять запросов. Остальные 4xx (400/401/402/403/404) не повторяются.
    Итог неудачи: AIError с видом ошибки (kind), по нему owner выбирает текст владельцу."""
    global _ai_sem
    if not base.CFG.openrouter_api_key or not base.CFG.openrouter_model:
        raise AIError("OpenRouter не настроен (OPENROUTER_API_KEY / OPENROUTER_MODEL)")
    if _ai_sem is None:
        _ai_sem = asyncio.Semaphore(1)
    payload = build_ai_payload(get_prompt(), data, fmt)
    attempts = len(AI_RETRY_PAUSES) + 1
    last_err = "нет ответа"
    last_kind = "other"
    async with _ai_sem:
        for attempt in range(1, attempts + 1):
            last_kind = "other"
            try:
                r = await client.post(OPENROUTER_URL, headers=ai_headers(), json=payload, timeout=AI_TIMEOUT)
            except httpx.HTTPError as e:
                last_err = f"сеть/таймаут: {e!r}"
                log.warning("OpenRouter, попытка %d/%d: %s", attempt, attempts, last_err)
            else:
                body = _safe_json(r)
                status = r.status_code
                err_in_body = isinstance(body, dict) and isinstance(body.get("error"), dict) and not body.get("choices")
                if err_in_body and status == 200:                  # ошибка внутри HTTP 200
                    code = body["error"].get("code")
                    status = int(code) if str(code).isdigit() else 502
                if status == 200:
                    try:
                        choice = body["choices"][0]
                        text = (choice["message"]["content"] or "").strip()
                    except (KeyError, IndexError, TypeError):
                        text, choice = "", {}
                    if text:
                        if choice.get("finish_reason") == "length":
                            log.warning("OpenRouter: ответ обрезан по лимиту max_tokens=%d, "
                                       "увеличьте OPENROUTER_MAX_TOKENS", base.CFG.openrouter_max_tokens)
                        return text
                    last_err = "пустой или неожиданный ответ нейросети"
                    log.warning("OpenRouter, попытка %d/%d: %s", attempt, attempts, last_err)
                elif status == 429 or status == 408 or status >= 500:
                    last_err = f"HTTP {status}: {_error_text(body, r)}"
                    last_kind = "limit" if status == 429 else "other"
                    log.warning("OpenRouter, попытка %d/%d: %s", attempt, attempts, last_err)
                else:
                    hint = {401: " (проверьте OPENROUTER_API_KEY)", 402: " (недостаточно кредитов)",
                            403: " (доступ запрещён или модерация)", 404: " (проверьте OPENROUTER_MODEL)"}.get(status, "")
                    kind = {401: "auth", 402: "credits", 403: "auth", 404: "model"}.get(status, "other")
                    raise AIError(f"HTTP {status}{hint}: {_error_text(body, r)}", kind=kind)
            if attempt < attempts:
                pause = AI_RETRY_PAUSES[attempt - 1]
                log.info("OpenRouter: пауза %.0f с перед повтором", pause)
                await transport._sleep(pause)
    raise AIError(f"после {attempts} попыток: {last_err}", kind=last_kind)


async def check_model_vision(client: httpx.AsyncClient) -> None:
    """Проверка при старте по каталогу OpenRouter.

    Модель без входа «image»: владельцу уходит сообщение и бот останавливается (SystemExit).
    Модели нет в каталоге: предупреждение в журнал и владельцу, запуск продолжается
    (бывают псевдонимы вроде openrouter/auto). Каталог недоступен: проверка пропускается."""
    try:
        r = await client.get(OPENROUTER_MODELS_URL, timeout=15)
        if r.status_code != 200:
            log.info("проверка модели пропущена: HTTP %s", r.status_code)
            return
        catalog = r.json().get("data") or []
    except (httpx.HTTPError, ValueError, AttributeError) as e:
        log.info("проверка модели пропущена: %r", e)
        return
    for m in catalog:
        if not isinstance(m, dict) or m.get("id") != base.CFG.openrouter_model:
            continue
        mods = (m.get("architecture") or {}).get("input_modalities") or []
        if "image" in mods:
            log.info("модель %s заявляет поддержку изображений", base.CFG.openrouter_model)
            return
        msg = (f"Бот остановлен: модель {base.CFG.openrouter_model} не принимает изображения "
               f"(input_modalities={mods}). Укажите в OPENROUTER_MODEL модель с поддержкой image: "
               "https://openrouter.ai/models")
        await alert_owner(msg, force=True)
        raise SystemExit(msg)
    await alert_owner(f"Предупреждение: модель {base.CFG.openrouter_model} не найдена в каталоге OpenRouter. "
                      "Проверьте OPENROUTER_MODEL; если это псевдоним, можно игнорировать.",
                      key="model_not_found", every=3600.0)


# =====================================================================
# 10. ОБРАБОТКА СООБЩЕНИЙ ВЛАДЕЛЬЦА И ИЗОБРАЖЕНИЙ
# =====================================================================
def ai_error_text(e: Exception) -> str:
    """Сообщение владельцу об ошибке распознавания: суть и причина (секреты замаскированы)."""
    head = {
        "limit": "Нейросеть отвечает 429 (слишком много запросов): пять попыток не помогли. Картинку можно прислать позже.",
        "credits": "На счёте OpenRouter недостаточно кредитов, изображение не обработано.",
        "auth": "OpenRouter не принял ключ или доступ запрещён, изображение не обработано.",
        "model": "Модель OpenRouter не найдена или недоступна, изображение не обработано.",
    }.get(getattr(e, "kind", "other"), MSG["ai_error"])
    return f"{head}\nПричина: {redact(str(e))[:300]}"
