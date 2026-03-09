import asyncio
import logging
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from typing import Optional, List, Tuple

import requests
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import FSInputFile, Message, BufferedInputFile

# =========================
# CONFIG
# =========================
BOT_TOKEN = "7923754810:AAEdfhrn8n7k-6WOSjV9OGEigP9uRYSrjk0"
REQUEST_DELAY_SECONDS = 1.2
HTTP_TIMEOUT = 25
MAX_LINKS_PER_REQUEST = 500
MAX_TEXT_CHARS = 50000

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# =========================
# BOT
# =========================
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не найден. Добавь переменную окружения BOT_TOKEN на хостинге.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =========================
# STATELESS STATS
# =========================
TOTAL_TEXT_REQUESTS = 0
TOTAL_FILE_REQUESTS = 0
TOTAL_LINKS_PROCESSED = 0
TOTAL_SUCCESS = 0
TOTAL_ERRORS = 0

URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)
VKCC_RE = re.compile(r"https://vk\.cc/[A-Za-z0-9]+", re.IGNORECASE)

HELP_TEXT = (
    "Привет. Я сокращаю ссылки через vk.cc.\n\n"
    "Что умею:\n"
    "1) Одна ссылка сообщением\n"
    "2) Много ссылок текстом — каждая ссылка с новой строки\n"
    "3) .txt файл со ссылками\n\n"
    "Команды:\n"
    "/start — запуск\n"
    "/help — помощь\n"
    "/ping — проверка\n"
    "/stats — статистика с момента запуска\n\n"
    "Формат:\n"
    "https://site1.ru/page1\n"
    "https://site2.ru/page2\n"
    "https://site3.ru/page3\n\n"
    "На выходе:\n"
    "https://vk.cc/xxxxx\n"
    "https://vk.cc/yyyyy\n"
    "ERROR\n\n"
    "Важно: способ без официального API. Если vk.cc начнет требовать капчу или изменит сайт, могут быть ERROR."
)


@dataclass
class ShortenResult:
    source: str
    short: Optional[str]
    error: Optional[str] = None


def normalize_lines(text: str) -> List[str]:
    lines = [line.strip() for line in text.replace("\r", "\n").split("\n")]
    return [line for line in lines if line]



def parse_links_from_text(text: str) -> List[str]:
    text = text.strip()
    if not text:
        return []

    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS]

    lines = normalize_lines(text)
    links: List[str] = []

    for line in lines:
        found = URL_RE.findall(line)
        if found:
            links.extend(found)

    # Убираем дубли, сохраняя порядок
    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    return unique_links


class VkCcShortener:
    def __init__(self, timeout: int = HTTP_TIMEOUT):
        self.session = requests.Session()
        self.timeout = timeout
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Referer": "https://vk.cc/",
            "Origin": "https://vk.cc",
        }

    def _extract_short(self, text: str) -> Optional[str]:
        match = VKCC_RE.search(text)
        if match:
            return match.group(0)
        return None

    def shorten(self, long_url: str) -> ShortenResult:
        endpoints: List[Tuple[str, dict]] = [
            ("https://vk.cc/", {"url": long_url}),
            ("https://vk.cc/?act=shorten", {"url": long_url}),
        ]

        for endpoint, data in endpoints:
            try:
                response = self.session.post(
                    endpoint,
                    data=data,
                    headers=self.headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                )

                if response.status_code != 200:
                    continue

                short_link = self._extract_short(response.text)
                if short_link:
                    return ShortenResult(source=long_url, short=short_link)
            except Exception as e:
                logger.warning("Ошибка запроса к %s: %s", endpoint, e)

        return ShortenResult(source=long_url, short=None, error="Не удалось получить vk.cc")


shortener = VkCcShortener()


async def process_links(links: List[str]) -> List[ShortenResult]:
    global TOTAL_LINKS_PROCESSED, TOTAL_SUCCESS, TOTAL_ERRORS
    results: List[ShortenResult] = []

    for idx, link in enumerate(links, start=1):
        result = await asyncio.to_thread(shortener.shorten, link)
        results.append(result)

        TOTAL_LINKS_PROCESSED += 1
        if result.short:
            TOTAL_SUCCESS += 1
            logger.info("%s | OK | %s -> %s", idx, link, result.short)
        else:
            TOTAL_ERRORS += 1
            logger.warning("%s | ERROR | %s | %s", idx, link, result.error)

        await asyncio.sleep(REQUEST_DELAY_SECONDS)

    return results



def build_output_text(results: List[ShortenResult]) -> str:
    lines = []
    for item in results:
        lines.append(item.short if item.short else "ERROR")
    return "\n".join(lines) + ("\n" if lines else "")



def build_verbose_text(results: List[ShortenResult]) -> str:
    lines = []
    for item in results:
        if item.short:
            lines.append(item.short)
        else:
            lines.append(f"ERROR | {item.source}")
    return "\n".join(lines) + ("\n" if lines else "")


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(HELP_TEXT)


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    await message.answer(HELP_TEXT)


@dp.message(Command("ping"))
async def cmd_ping(message: Message) -> None:
    await message.answer("Бот на связи.")


@dp.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    await message.answer(
        "Статистика с момента запуска:\n"
        f"Текстовых запросов: {TOTAL_TEXT_REQUESTS}\n"
        f"Файловых запросов: {TOTAL_FILE_REQUESTS}\n"
        f"Всего ссылок обработано: {TOTAL_LINKS_PROCESSED}\n"
        f"Успешно: {TOTAL_SUCCESS}\n"
        f"Ошибок: {TOTAL_ERRORS}"
    )


@dp.message(F.document)
async def handle_document(message: Message) -> None:
    global TOTAL_FILE_REQUESTS
    TOTAL_FILE_REQUESTS += 1

    document = message.document
    if not document.file_name or not document.file_name.lower().endswith(".txt"):
        await message.answer("Отправь .txt файл со ссылками.")
        return

    await message.answer("Файл получен. Начинаю обработку.")

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, document.file_name)
        output_path = os.path.join(temp_dir, f"short_{document.file_name}")
        verbose_output_path = os.path.join(temp_dir, f"verbose_{document.file_name}")

        try:
            file_info = await bot.get_file(document.file_id)
            await bot.download_file(file_info.file_path, destination=input_path)

            with open(input_path, "r", encoding="utf-8") as f:
                raw_text = f.read()

            links = parse_links_from_text(raw_text)
            if not links:
                await message.answer("В файле не найдено ни одной ссылки.")
                return

            if len(links) > MAX_LINKS_PER_REQUEST:
                await message.answer(
                    f"Слишком много ссылок за раз. Максимум: {MAX_LINKS_PER_REQUEST}."
                )
                return

            results = await process_links(links)
            output_text = build_output_text(results)
            verbose_text = build_verbose_text(results)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(output_text)

            with open(verbose_output_path, "w", encoding="utf-8") as f:
                f.write(verbose_text)

            success_count = sum(1 for x in results if x.short)
            error_count = len(results) - success_count

            await message.answer_document(
                FSInputFile(output_path),
                caption=(
                    f"Готово.\n"
                    f"Всего: {len(results)}\n"
                    f"Успешно: {success_count}\n"
                    f"Ошибок: {error_count}\n\n"
                    f"В файле — только короткие ссылки и ERROR."
                ),
            )

            if error_count:
                await message.answer_document(
                    FSInputFile(verbose_output_path),
                    caption="Доп. файл: где были ошибки, там строка вида ERROR | исходная_ссылка",
                )

        except Exception as e:
            logger.exception("Ошибка обработки файла")
            await message.answer(f"Ошибка при обработке файла: {e}")


@dp.message(F.text)
async def handle_text(message: Message) -> None:
    global TOTAL_TEXT_REQUESTS
    TOTAL_TEXT_REQUESTS += 1

    text = (message.text or "").strip()
    links = parse_links_from_text(text)

    if not links:
        await message.answer(
            "Не нашёл ссылок. Пришли одну ссылку или список ссылок, каждая с новой строки."
        )
        return

    if len(links) > MAX_LINKS_PER_REQUEST:
        await message.answer(
            f"Слишком много ссылок в одном сообщении. Максимум: {MAX_LINKS_PER_REQUEST}."
        )
        return

    if len(links) == 1:
        await message.answer("Обрабатываю ссылку.")
        result = await asyncio.to_thread(shortener.shorten, links[0])

        global TOTAL_LINKS_PROCESSED, TOTAL_SUCCESS, TOTAL_ERRORS
        TOTAL_LINKS_PROCESSED += 1
        if result.short:
            TOTAL_SUCCESS += 1
            await message.answer(result.short)
        else:
            TOTAL_ERRORS += 1
            await message.answer("ERROR")
        return

    await message.answer(f"Получил {len(links)} ссылок. Начинаю обработку.")
    results = await process_links(links)

    output_text = build_output_text(results)
    success_count = sum(1 for x in results if x.short)
    error_count = len(results) - success_count

    if len(output_text) <= 3500:
        await message.answer(
            f"Готово.\nУспешно: {success_count}\nОшибок: {error_count}\n\n{output_text}"
        )
        return

    file_bytes = output_text.encode("utf-8")
    file = BufferedInputFile(file_bytes, filename="short_links.txt")
    await message.answer_document(
        file,
        caption=(
            f"Готово.\n"
            f"Всего: {len(results)}\n"
            f"Успешно: {success_count}\n"
            f"Ошибок: {error_count}"
        ),
    )


async def main() -> None:
    logger.info("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
