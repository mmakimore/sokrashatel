import asyncio
import csv
import io
import logging
import os
import random
import re
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.filters import CommandStart
from aiogram.types import BufferedInputFile, Document, Message
from dotenv import load_dotenv


load_dotenv()


# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "1.2"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "25"))
MAX_LINKS_PER_REQUEST = int(os.getenv("MAX_LINKS_PER_REQUEST", "500"))
MAX_TEXT_CHARS = int(os.getenv("MAX_TEXT_CHARS", "50000"))
MAX_WORKERS = max(1, int(os.getenv("MAX_WORKERS", "1")))
PROGRESS_EVERY = max(1, int(os.getenv("PROGRESS_EVERY", "15")))
RESULT_FILENAME = os.getenv("RESULT_FILENAME", "short_links.txt")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
OUTPUT_ERRORS_AS_ERROR = os.getenv("OUTPUT_ERRORS_AS_ERROR", "true").lower() == "true"
ALLOW_DUPLICATE_LINKS = os.getenv("ALLOW_DUPLICATE_LINKS", "false").lower() == "true"

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN не найден. Создай .env файл или переменную окружения BOT_TOKEN."
    )

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

VKCC_PATTERN = re.compile(r"https://vk\.cc/[A-Za-z0-9]+")
URL_PATTERN = re.compile(r"https?://[^\s<>'\"]+")


# =========================
# RUNTIME STATE
# =========================
app_started_at = time.time()
user_stats = defaultdict(lambda: {
    "requests": 0,
    "links_total": 0,
    "success_total": 0,
    "errors_total": 0,
    "last_seen": 0,
})


# =========================
# DATA MODELS
# =========================
@dataclass
class ShortenResult:
    original: str
    short: Optional[str]
    error: Optional[str] = None


# =========================
# HELPERS
# =========================
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


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


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
    text = text.strip()
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
    match = VKCC_PATTERN.search(text)
    return match.group(0) if match else None


def format_output_lines(results: Sequence[ShortenResult]) -> List[str]:
    lines = []
    for item in results:
        if item.short:
            lines.append(item.short)
        else:
            lines.append("ERROR" if OUTPUT_ERRORS_AS_ERROR else f"ERROR: {item.error or 'unknown'}")
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


async def shorten_one(session: aiohttp.ClientSession, long_url: str) -> ShortenResult:
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

    for endpoint, payload in endpoints:
        for attempt in range(1, 4):
            try:
                async with session.post(
                    endpoint,
                    data=payload,
                    headers=headers,
                    allow_redirects=True,
                    timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT),
                ) as response:
                    body = await response.text()

                    short_link = pick_first_short_link(body)
                    if short_link:
                        return ShortenResult(original=long_url, short=short_link)

                    if response.status in {429, 403}:
                        last_error = f"HTTP {response.status}"
                    else:
                        last_error = f"HTTP {response.status} / no short link"

            except asyncio.TimeoutError:
                last_error = "timeout"
            except aiohttp.ClientError as exc:
                last_error = f"client error: {exc}"
            except Exception as exc:  # pragma: no cover
                last_error = f"unexpected error: {exc}"

            await asyncio.sleep(min(2.5, REQUEST_DELAY_SECONDS * attempt + random.uniform(0.15, 0.45)))

    return ShortenResult(original=long_url, short=None, error=last_error)


async def shorten_many(
    links: Sequence[str],
    progress_message: Message,
) -> List[ShortenResult]:
    results: List[Optional[ShortenResult]] = [None] * len(links)
    connector = aiohttp.TCPConnector(limit=max(2, MAX_WORKERS))

    async with aiohttp.ClientSession(connector=connector) as session:
        if MAX_WORKERS == 1:
            for idx, link in enumerate(links, start=1):
                results[idx - 1] = await shorten_one(session, link)
                if idx % PROGRESS_EVERY == 0 or idx == len(links):
                    await progress_message.edit_text(
                        f"Обработка: {idx}/{len(links)}"
                    )
                await asyncio.sleep(REQUEST_DELAY_SECONDS)
        else:
            semaphore = asyncio.Semaphore(MAX_WORKERS)
            done = 0
            done_lock = asyncio.Lock()

            async def worker(position: int, link: str) -> None:
                nonlocal done
                async with semaphore:
                    results[position] = await shorten_one(session, link)
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
                async with done_lock:
                    done += 1
                    if done % PROGRESS_EVERY == 0 or done == len(links):
                        try:
                            await progress_message.edit_text(f"Обработка: {done}/{len(links)}")
                        except Exception:
                            pass

            await asyncio.gather(*(worker(i, link) for i, link in enumerate(links)))

    return [item for item in results if item is not None]


async def send_result(message: Message, results: Sequence[ShortenResult]) -> None:
    lines = format_output_lines(results)
    text_result = "\n".join(lines).strip() or "ERROR"

    if len(text_result) <= 3500 and len(lines) <= 80:
        for part in chunk_text(text_result, 3500):
            await message.answer(part)
        return

    binary = text_result.encode("utf-8")
    file = BufferedInputFile(binary, filename=RESULT_FILENAME)
    await message.answer_document(file, caption="Готово. Отправляю файл с результатом.")


async def read_uploaded_file(document: Document) -> str:
    telegram_file = await bot.get_file(document.file_id)
    file_buffer = io.BytesIO()
    await bot.download_file(telegram_file.file_path, destination=file_buffer)
    file_buffer.seek(0)
    raw = file_buffer.read()
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


async def handle_links(message: Message, links: Sequence[str], source_name: str) -> None:
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
        f"Принял {len(links)} ссылок из {escape_html(source_name)}. Начинаю обработку..."
    )
    results = await shorten_many(links, progress)

    success_count = sum(1 for item in results if item.short)
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


# =========================
# COMMANDS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
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


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    text = (
        "<b>Как пользоваться</b>\n\n"
        "1. Одна ссылка: просто пришли её сообщением.\n"
        "2. Много ссылок: каждая с новой строки.\n"
        "3. Файл: .txt или .csv с ссылками.\n\n"
        "Бот возвращает только короткие ссылки или ERROR.\n"
        "Если VK начнёт блокировать веб-форму vk.cc, часть ссылок может вернуться как ERROR."
    )
    await message.answer(text)


@dp.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    await message.answer("pong")


@dp.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    user_id = message.from_user.id if message.from_user else 0
    await message.answer(format_stats_for_user(user_id))


# =========================
# INPUT HANDLERS
# =========================
@dp.message(F.document)
async def handle_document(message: Message) -> None:
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


@dp.message(F.text)
async def handle_text_message(message: Message) -> None:
    text = (message.text or "").strip()
    if not text:
        await message.answer("Пустое сообщение.")
        return

    links = extract_links_from_text(text)
    if not links:
        await message.answer("Не нашёл ссылок в сообщении.")
        return

    await handle_links(message, links, source_name="сообщения")


async def on_startup() -> None:
    me = await bot.get_me()
    logger.info("Bot started: @%s (%s)", me.username, me.id)


async def main() -> None:
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
