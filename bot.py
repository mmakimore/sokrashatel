import asyncio
import logging
import os
import re
import tempfile
from typing import Optional

import requests
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Document, FSInputFile, Message

# =========================
# НАСТРОЙКИ
# =========================
BOT_TOKEN = 7923754810:AAEdfhrn8n7k-6WOSjV9OGEigP9uRYSrjk0

# Пауза между запросами к vk.cc
REQUEST_DELAY_SECONDS = 1.0

# Таймаут HTTP-запросов
HTTP_TIMEOUT = 20

# =========================
# ЛОГИ
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================
# TG BOT
# =========================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def extract_vkcc_link(text: str) -> Optional[str]:
    """
    Ищет в тексте первую ссылку вида https://vk.cc/xxxxxx
    """
    match = re.search(r"https://vk\.cc/[A-Za-z0-9]+", text)
    if match:
        return match.group(0)
    return None


def shorten_vkcc(long_url: str) -> Optional[str]:
    """
    Пытается сократить ссылку через vk.cc без API-токена.
    Возвращает короткую ссылку или None.
    """
    session = requests.Session()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://vk.cc/",
        "Origin": "https://vk.cc",
    }

    endpoints = [
        ("https://vk.cc/", {"url": long_url}),
        ("https://vk.cc/?act=shorten", {"url": long_url}),
    ]

    for endpoint, data in endpoints:
        try:
            response = session.post(
                endpoint,
                data=data,
                headers=headers,
                timeout=HTTP_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code != 200:
                continue

            short_link = extract_vkcc_link(response.text)
            if short_link:
                return short_link

        except Exception as e:
            logging.warning("Ошибка запроса к %s: %s", endpoint, e)

    return None


@dp.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        "Привет.\n\n"
        "Пришли .txt файл со ссылками, где каждая ссылка с новой строки.\n"
        "Я верну .txt файл только с короткими ссылками vk.cc.\n\n"
        "Также можно отправить одну ссылку сообщением."
    )


@dp.message(F.document)
async def handle_document(message: Message) -> None:
    document: Document = message.document

    if not document.file_name or not document.file_name.lower().endswith(".txt"):
        await message.answer("Отправь именно .txt файл.")
        return

    await message.answer("Файл получен. Начинаю обработку.")

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, document.file_name)
        output_path = os.path.join(temp_dir, f"short_{document.file_name}")

        try:
            file_info = await bot.get_file(document.file_id)
            await bot.download_file(file_info.file_path, destination=input_path)

            with open(input_path, "r", encoding="utf-8") as f:
                links = [line.strip() for line in f if line.strip()]

            if not links:
                await message.answer("Файл пустой.")
                return

            success_count = 0
            error_count = 0

            with open(output_path, "w", encoding="utf-8") as out:
                for idx, link in enumerate(links, start=1):
                    short_link = shorten_vkcc(link)

                    if short_link:
                        out.write(short_link + "\n")
                        success_count += 1
                        logging.info("%s | OK | %s -> %s", idx, link, short_link)
                    else:
                        out.write("ERROR\n")
                        error_count += 1
                        logging.warning("%s | ERROR | %s", idx, link)

                    await asyncio.sleep(REQUEST_DELAY_SECONDS)

            result_file = FSInputFile(output_path)
            await message.answer_document(
                result_file,
                caption=(
                    f"Готово.\n"
                    f"Всего ссылок: {len(links)}\n"
                    f"Успешно: {success_count}\n"
                    f"Ошибок: {error_count}"
                )
            )

        except Exception as e:
            logging.exception("Ошибка обработки файла")
            await message.answer(f"Ошибка при обработке файла: {e}")


@dp.message(F.text)
async def handle_text(message: Message) -> None:
    text = message.text.strip()

    if not (text.startswith("http://") or text.startswith("https://")):
        await message.answer("Пришли ссылку, которая начинается с http:// или https://")
        return

    await message.answer("Обрабатываю ссылку...")

    short_link = shorten_vkcc(text)

    if short_link:
        await message.answer(short_link)
    else:
        await message.answer("Не удалось сократить ссылку. Вернулся ERROR.")


async def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PASTE_TELEGRAM_BOT_TOKEN_HERE":
        raise ValueError("Не задан BOT_TOKEN")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
