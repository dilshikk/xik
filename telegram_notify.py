"""Отправка уведомлений о проходах (вход/выход) в Telegram-группу.

Включается переменными TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID. Если хотя бы
одна из них не задана, отправка отключена. Ошибки отправки только пишутся в
лог: приём событий от камеры от них не зависит.

Формат сообщения: фото сотрудника (если прислано камерой), под ним:
    #Имя_Фамилия_НомерСотрудника
    дд.мм.гггг чч:мм #checkin
и кнопка «События за день». По нажатию бот показывает все проходы этого
сотрудника за тот день (берёт их из таблицы hik_events).

Нажатия принимаются через long polling (getUpdates), поэтому шлюзу не нужен
HTTPS. Запускайте шлюз в ОДНОМ процессе: два процесса с одним ботом
мешают друг другу (ошибка 409 Conflict).
"""
import asyncio
import json
import logging
import re
from datetime import date, datetime, time, timedelta

import httpx
from sqlalchemy import select

from attendance import LOCAL_TZ, detect_direction, extract_events, parse_local_time
from config import FILES_DIR, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from db import HikEvent, SessionLocal

logger = logging.getLogger("hikvision.telegram")

API_URL = "https://api.telegram.org/bot{token}/{method}"
IMAGE_EXT = (".jpg", ".jpeg", ".png", ".bmp")

# Направление прохода -> хэштег в подписи и слово в списке за день
HASHTAGS = {"in": "#checkin", "out": "#checkout"}
DIRECTION_LABELS = {"in": "вход", "out": "выход"}

CALLBACK_PREFIX = "day"
CALLBACK_MAX_BYTES = 64  # ограничение Telegram на callback_data
ALERT_LIMIT = 200  # ограничение Telegram на текст всплывающего окна
POLL_TIMEOUT = 50
RETRY_DELAY = 5
# Запас на расхождение часов терминала и сервера при выборке за день
DAY_MARGIN = timedelta(hours=2)

# В хэштеге допустимы только буквы, цифры и "_"
NON_WORD = re.compile(r"\W+")


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def api_url(method: str) -> str:
    return API_URL.format(token=TELEGRAM_BOT_TOKEN, method=method)


def employee_no(details: dict) -> str:
    return str(details.get("employeeNoString") or "").strip()


def make_hashtag(details: dict) -> str:
    """#Иван_Петров_1024: номер не даёт склеить однофамильцев."""
    name = str(details.get("name") or "").strip() or "Сотрудник"
    tag = NON_WORD.sub("_", f"{name} {employee_no(details)}").strip("_")
    return f"#{tag or 'Сотрудник'}"


def build_caption(details: dict, local: datetime, direction: str | None) -> str:
    hashtag = HASHTAGS.get(direction or "", "#событие")
    return f"{make_hashtag(details)}\n{local.strftime('%d.%m.%Y %H:%M')} {hashtag}"


def day_button(details: dict, local: datetime) -> str | None:
    no = employee_no(details)
    if not no:
        return None
    data = f"{CALLBACK_PREFIX}:{no}:{local.date().isoformat()}"
    if len(data.encode()) > CALLBACK_MAX_BYTES:
        return None
    keyboard = {"inline_keyboard": [[{"text": "События за день", "callback_data": data}]]}
    return json.dumps(keyboard)


def pick_photo(files: list[str]) -> str | None:
    """Файл с фото лица из этого же запроса камеры, если он есть."""
    for name in files:
        if name.lower().endswith(IMAGE_EXT):
            return name
    return None


async def send_item(client: httpx.AsyncClient, item: dict, files: list[str]) -> None:
    details = item["AccessControllerEvent"]
    local = parse_local_time(item)
    caption = build_caption(details, local, detect_direction(details))
    data = {"chat_id": TELEGRAM_CHAT_ID}
    markup = day_button(details, local)
    if markup:
        data["reply_markup"] = markup
    photo_name = pick_photo(files)

    if photo_name:
        with (FILES_DIR / photo_name).open("rb") as fh:
            response = await client.post(
                api_url("sendPhoto"),
                data={**data, "caption": caption},
                files={"photo": (photo_name, fh, "image/jpeg")},
            )
    else:
        response = await client.post(api_url("sendMessage"), data={**data, "text": caption})
    if response.status_code != 200:
        logger.warning("Telegram API error %s: %s", response.status_code, response.text)


async def forward_payload(payload: list, files: list[str]) -> None:
    """Отправляет уведомления обо всех проходах из тела события. Не бросает исключений."""
    if not telegram_enabled():
        return
    events = extract_events(payload)
    if not events:
        return
    async with httpx.AsyncClient(timeout=15) as client:
        for item in events:
            try:
                await send_item(client, item, files)
            except Exception:
                logger.exception("Failed to send Telegram notification")


# ---------- Кнопка «События за день» ----------


async def load_day_events(wanted_no: str, day: date) -> list[tuple[datetime, str | None]]:
    """Проходы сотрудника за день (по времени терминала), без повторов."""
    start = datetime.combine(day, time.min, LOCAL_TZ)
    end = start + timedelta(days=1)
    stmt = (
        select(HikEvent.payload)
        .where(HikEvent.event_type == "AccessControllerEvent")
        .where(HikEvent.received_at >= start - DAY_MARGIN)
        .where(HikEvent.received_at < end + DAY_MARGIN)
        .where(
            HikEvent.payload.contains(
                [{"AccessControllerEvent": {"employeeNoString": wanted_no}}]
            )
        )
        .order_by(HikEvent.received_at)
    )
    async with SessionLocal() as session:
        payloads = (await session.execute(stmt)).scalars().all()

    seen: set[tuple[datetime, str | None]] = set()
    for payload in payloads:
        for item in extract_events(payload):
            details = item["AccessControllerEvent"]
            if employee_no(details) != wanted_no:
                continue
            local = parse_local_time(item)
            if local.date() != day:
                continue
            # Камера иногда шлёт одно событие дважды
            seen.add((local.replace(microsecond=0), detect_direction(details)))
    return sorted(seen, key=lambda row: row[0])


def format_day_report(rows: list[tuple[datetime, str | None]], day: date) -> str:
    title = day.strftime("%d.%m.%Y")
    if not rows:
        return f"{title}: событий нет"
    lines = [f"{title}, событий: {len(rows)}"]
    lines += [
        f"{moment.strftime('%H:%M')} {DIRECTION_LABELS.get(direction or '', 'событие')}"
        for moment, direction in rows
    ]
    return "\n".join(lines)


async def answer_callback(
    client: httpx.AsyncClient, query_id: str, text: str, alert: bool = False
) -> None:
    await client.post(
        api_url("answerCallbackQuery"),
        data={"callback_query_id": query_id, "text": text, "show_alert": json.dumps(alert)},
    )


async def handle_callback(client: httpx.AsyncClient, query: dict) -> None:
    query_id = query["id"]
    prefix, _, rest = str(query.get("data") or "").partition(":")
    wanted_no, _, day_str = rest.rpartition(":")
    message = query.get("message") or {}
    chat_id = str((message.get("chat") or {}).get("id", ""))

    # Отвечаем только на кнопки из нашей группы
    if prefix != CALLBACK_PREFIX or not wanted_no or chat_id != str(TELEGRAM_CHAT_ID):
        await answer_callback(client, query_id, "Кнопка недоступна")
        return
    try:
        day = date.fromisoformat(day_str)
    except ValueError:
        await answer_callback(client, query_id, "Неверная дата")
        return

    report = format_day_report(await load_day_events(wanted_no, day), day)
    if len(report) <= ALERT_LIMIT:
        # Всплывающее окно видит только нажавший, группа не засоряется
        await answer_callback(client, query_id, report, alert=True)
        return
    await answer_callback(client, query_id, "Список отправлен в чат")
    await client.post(
        api_url("sendMessage"),
        data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": report,
            "reply_parameters": json.dumps(
                {"message_id": message.get("message_id"), "allow_sending_without_reply": True}
            ),
        },
    )


async def poll_callbacks() -> None:
    """Принимает нажатия кнопок, пока работает шлюз. Не бросает исключений."""
    if not telegram_enabled():
        return
    offset = 0
    async with httpx.AsyncClient(timeout=POLL_TIMEOUT + 10) as client:
        # getUpdates не работает, если у бота задан webhook
        try:
            await client.post(api_url("deleteWebhook"))
        except Exception:
            logger.exception("Failed to delete Telegram webhook")
        while True:
            try:
                response = await client.post(
                    api_url("getUpdates"),
                    data={
                        "offset": offset,
                        "timeout": POLL_TIMEOUT,
                        "allowed_updates": json.dumps(["callback_query"]),
                    },
                )
                body = response.json()
                if not body.get("ok"):
                    logger.warning("Telegram getUpdates error: %s", body)
                    await asyncio.sleep(RETRY_DELAY)
                    continue
                for update in body["result"]:
                    offset = update["update_id"] + 1
                    query = update.get("callback_query")
                    if not query:
                        continue
                    try:
                        await handle_callback(client, query)
                    except Exception:
                        logger.exception("Failed to handle Telegram button")
            except Exception:
                logger.exception("Telegram polling failed")
                await asyncio.sleep(RETRY_DELAY)
