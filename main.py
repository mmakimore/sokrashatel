import asyncio
import csv
import io
import logging
import os
import re
import time
from collections import defaultdict
from html import escape
from typing import Iterable, List, Optional, Sequence

import requests
from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import Command, Text
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "1.2"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
MAX_LINKS_PER_REQUEST = int(os.getenv("MAX_LINKS_PER_REQUEST", "500"))
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "50000"))
RESULT_FILENAME = os.getenv("RESULT_FILENAME", "short_links.txt")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
OUTPUT_ERRORS_AS_ERROR = os.getenv("OUTPUT_ERRORS_AS_ERROR", "true").lower() == "true"
ALLOW_DUPLICATE_LINKS = os.getenv("ALLOW_DUPLICATE_LINKS", "false").lower() == "true"

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не найден. Добавь его в .env файл.")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

VKCC_PATTERN = re.compile(r"https://vk\.cc/[A-Za-z0-9]+")
URL_PATTERN = re.compile(r"https?://[^\s<>'\"]+")

app_started_at = time.time()
user_stats = defaultdict(lambda: {
    "requests": 0,
    "links_total": 0,
    "success_total": 0,
    "errors_total": 0,
    "last_seen": 0,
})


def chunk_text(text: str, size: int = 3500) -> List[str]:
    if len(text) <= size:
        return [text]
    parts = []
    current = []
    current_len = 0
    for line in text.splitlines(True):
        if current_len + len(line) > size and current:
            parts.append("".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += len(line)
    if current:
        parts.append("".join(current))
    return parts


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    if ALLOW_DUPLICATE_LINKS:
        return list(items)
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def extract_links_from_text(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]
    links = URL_PATTERN.findall(text)
    cleaned = [link.rstrip(",.;)") for link in links]
    return unique_preserve_order(cleaned)


def parse_csv_links(raw_text: str) -> List[str]:
    found: List[str] = []
    stream = io.StringIO(raw_text)
    try:
        reader = csv.reader(stream)
        for row in reader:
            for cell in row:
                found.extend(extract_links_from_text(cell))
    except Exception:
        return []
    return unique_preserve_order(found)


def pick_first_short_link(text: str) -> Optional[str]:
    match = VKCC_PATTERN.search(text or "")
    return match.group(0) if match else None


def format_output_lines(results: Sequence[dict]) -> List[str]:
    lines = []
    for item in results:
        short = item.get("short")
        err = item.get("error") or "unknown"
        if short:
            lines.append(short)
        else:
            lines.append("ERROR" if OUTPUT_ERRORS_AS_ERROR else f"ERROR: {err}")
    return lines


def format_stats_for_user(user_id: int) -> str:
    stats = user_stats[user_id]
    uptime_sec = int(time.time() - app_started_at)
    return (
        "<b>Статистика</b>\n"
        f"Запросов: <b>{stats['requests']}</b>\n"
        f"Ссылок обработано: <b>{stats['links_total']}</b>\n"
        f"Успешно: <b>{stats['success_total']}</b>\n"
        f"Ошибок: <b>{stats['errors_total']}</b>\n"
        f"Аптайм бота: <b>{uptime_sec} сек</b>"
    )


def shorten_vkcc_sync(long_url: str) -> dict:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/132.0.0.0 Safari/537.36"
        ),
        "Referer": "https://vk.cc/",
        "Origin": "https://vk.cc",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    endpoints = [
        ("https://vk.cc/", {"url": long_url}),
        ("https://vk.cc/?act=shorten", {"url": long_url}),
    ]

    last_error = "unknown"
    session = requests.Session()

    for endpoint, payload in endpoints:
        for attempt in range(1, 4):
            try:
                response = session.post(
                    endpoint,
                    data=payload,
                    headers=headers,
                    timeout=HTTP_TIMEOUT,
                    allow_redirects=True,
                )
                short_link = pick_first_short_link(response.text)
                if short_link:
                    return {"original": long_url, "short": short_link, "error": None}

                if response.status_code in (429, 403):
                    last_error = f"HTTP {response.status_code}"
                else:
                    last_error = f"HTTP {response.status_code} / no short link"
            except requests.Timeout:
                last_error = "timeout"
            except requests.RequestException as exc:
                last_error = f"request error: {exc}"
            except Exception as exc:
                last_error = f"unexpected error: {exc}"

            time.sleep(min(2.5, REQUEST_DELAY_SECONDS * attempt + 0.2))

    return {"original": long_url, "short": None, "error": last_error}


async def shorten_many(links: Sequence[str], progress_message: types.Message) -> List[dict]:
    results: List[dict] = []
    loop = asyncio.get_running_loop()

    for idx, link in enumerate(links, start=1):
        result = await loop.run_in_executor(None, shorten_vkcc_sync, link)
        results.append(result)

        if idx % 15 == 0 or idx == len(links):
            try:
                await progress_message.edit_text(f"Обработка: {idx}/{len(links)}")
            except Exception:
                pass

        await asyncio.sleep(REQUEST_DELAY_SECONDS)

    return results


async def send_result(message: types.Message, results: Sequence[dict]) -> None:
    lines = format_output_lines(results)
    text_result = "\n".join(lines).strip() or "ERROR"

    if len(text_result) <= 3500 and len(lines) <= 80:
        for part in chunk_text(text_result, 3500):
            await message.answer(part)
        return

    binary = io.BytesIO(text_result.encode("utf-8"))
    binary.name = RESULT_FILENAME
    await message.answer_document(binary, caption="Готово. Отправляю файл с результатом.")


async def read_uploaded_file(document: types.Document) -> str:
    buffer = io.BytesIO()
    await document.download(destination_file=buffer)
    buffer.seek(0)
    raw = buffer.read()
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


async def handle_links(message: types.Message, links: Sequence[str], source_name: str) -> None:
    if not links:
        await message.answer("Не нашёл ни одной ссылки.")
        return

    if len(links) > MAX_LINKS_PER_REQUEST:
        await message.answer(
            f"Слишком много ссылок за раз: {len(links)}. Лимит: {MAX_LINKS_PER_REQUEST}."
        )
        return

    user_id = message.from_user.id if message.from_user else 0
    user_stats[user_id]["requests"] += 1
    user_stats[user_id]["links_total"] += len(links)
    user_stats[user_id]["last_seen"] = int(time.time())

    progress = await message.answer(
        f"Принял {len(links)} ссылок из {escape(source_name)}. Начинаю обработку..."
    )
    results = await shorten_many(links, progress)

    success_count = sum(1 for item in results if item.get("short"))
    error_count = len(results) - success_count
    user_stats[user_id]["success_total"] += success_count
    user_stats[user_id]["errors_total"] += error_count

    try:
        await progress.edit_text(
            f"Готово. Всего: {len(results)} | Успешно: {success_count} | Ошибок: {error_count}"
        )
    except Exception:
        pass

    await send_result(message, results)


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message) -> None:
    text = (
        "<b>VKCC Shortener Bot</b>\n\n"
        "Что умею:\n"
        "• сокращать одну ссылку сообщением\n"
        "• сокращать много ссылок текстом\n"
        "• принимать .txt и .csv файлы\n"
        "• возвращать только короткие ссылки\n"
        "• показывать прогресс и статистику\n\n"
        "Команды:\n"
        "/help — инструкция\n"
        "/ping — проверка, что бот жив\n"
        "/stats — твоя статистика\n\n"
        "Отправь ссылку, пачку ссылок или файл."
    )
    await message.answer(text)


@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message) -> None:
    text = (
        "<b>Как пользоваться</b>\n\n"
        "1. Одна ссылка: просто пришли её сообщением.\n"
        "2. Много ссылок: каждая с новой строки.\n"
        "3. Файл: .txt или .csv с ссылками.\n\n"
        "Бот возвращает только короткие ссылки или ERROR.\n"
        "Если VK начнёт блокировать веб-форму vk.cc, часть ссылок может вернуться как ERROR."
    )
    await message.answer(text)


@dp.message_handler(commands=["ping"])
async def cmd_ping(message: types.Message) -> None:
    await message.answer("pong")


@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    await message.answer(format_stats_for_user(user_id))


@dp.message_handler(content_types=types.ContentType.DOCUMENT)
async def handle_document(message: types.Message) -> None:
    document = message.document
    file_name = (document.file_name or "file").lower()

    if not (file_name.endswith(".txt") or file_name.endswith(".csv")):
        await message.answer("Поддерживаю только .txt и .csv файлы.")
        return

    raw_text = await read_uploaded_file(document)
    links = extract_links_from_text(raw_text)

    if not links and file_name.endswith(".csv"):
        links = parse_csv_links(raw_text)

    await handle_links(message, links, source_name=file_name)


@dp.message_handler(content_types=types.ContentType.TEXT)
async def handle_text_message(message: types.Message) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Пустое сообщение.")
        return

    if text.startswith("/"):
        return

    links = extract_links_from_text(text)
    if not links:
        await message.answer("Не нашёл ссылок в сообщении.")
        return

    await handle_links(message, links, source_name="сообщения")


async def on_startup(dispatcher: Dispatcher) -> None:
    me = await bot.get_me()
    logger.info("Bot started: @%s (%s)", me.username, me.id)


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
