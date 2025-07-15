import asyncio
import os
import uuid
from typing import Dict, Any, Union, Callable, Awaitable

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, BaseFilter
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
import database
import queue_client

class IsAdmin(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        return message.from_user.id in config.get_admin_ids()

async def handle_start(message: Message, bot_config: dict, db_name: str):
    user_id = message.from_user.id
    start_balance = bot_config.get("default_user_balance", 0)
    
    is_new_user = database.add_or_get_user(db_name, user_id, start_balance=start_balance)

    message_key = "welcome" if is_new_user else "welcome_back"
    welcome_message = config.get_message(bot_config, message_key)
    await message.answer(welcome_message)

async def handle_info_commands(message: Message, bot_config: dict):
    command = message.text.lstrip('/')
    info_message = config.get_message(bot_config, command)
    await message.answer(info_message)

async def handle_stats(message: Message, bot_config: dict, db_name: str):
    user_id = message.from_user.id
    stats = database.get_user_stats(db_name, user_id)

    if stats:
        stats_message = config.get_message(bot_config, "stats_template", **stats)
    else:
        database.add_or_get_user(db_name, user_id)
        stats_message = config.get_message(bot_config, "stats_template", balance=0, total_processed=0)
    
    await message.answer(stats_message)

async def handle_photo(message: Message, bot: Bot, bot_config: dict, db_name: str):
    user_id = message.from_user.id
    
    balance = database.get_user_balance(db_name, user_id)
    if balance is None or balance <= 0:
        error_message = config.get_message(bot_config, "no_credits")
        await message.answer(error_message)
        return

    photo = message.photo[-1] if message.photo else None
    document = message.document if message.document and message.document.mime_type.startswith("image/") else None
    
    if not (photo or document):
        return

    file_id = photo.file_id if photo else document.file_id
    file_size_mb = (photo.file_size if photo else document.file_size) / 1024 / 1024
    max_size = bot_config.get("max_file_size_mb", 20)

    if file_size_mb > max_size:
        error_message = config.get_message(bot_config, "invalid_file_size", max_size=max_size)
        await message.answer(error_message)
        return

    try:
        storage_path = bot_config["photo_storage_path"]
        if not os.path.exists(storage_path):
            os.makedirs(storage_path)
        
        file_info = await bot.get_file(file_id)
        file_ext = os.path.splitext(file_info.file_path)[1]
        downloaded_file_path = os.path.join(storage_path, f"{user_id}_{uuid.uuid4().hex}{file_ext}")
        
        await bot.download_file(file_info.file_path, destination=downloaded_file_path)

        if database.spend_credit(db_name, user_id):
            task_data = {"user_id": user_id, "original_path": downloaded_file_path}
            success = queue_client.enqueue_task(
                bot_config["redis_host"], bot_config["redis_port"],
                bot_config["redis_queue_name"], task_data
            )
            if success:
                success_message = config.get_message(bot_config, "enqueue_success")
                await message.answer(success_message)
            else:
                database.update_user_balance(db_name, user_id, 1)
                await message.answer("Произошла системная ошибка, попробуйте позже.")
        else:
            await message.answer(config.get_message(bot_config, "no_credits"))

    except Exception as e:
        await message.answer("Произошла непредвиденная ошибка. Попробуйте еще раз.")

async def handle_admin_balance_commands(message: Message, bot_config: dict, db_name: str):
    command_parts = message.text.split()
    if len(command_parts) != 3:
        await message.answer(config.get_message(bot_config, "admin_invalid_command"))
        return

    command, target_user_id_str, amount_str = command_parts
    try:
        target_user_id = int(target_user_id_str)
        amount = int(amount_str)
    except ValueError:
        await message.answer(config.get_message(bot_config, "admin_invalid_command"))
        return

    set_exact = command == "/set"
    new_balance = database.update_user_balance(db_name, target_user_id, amount, set_exact=set_exact)
    
    if new_balance is not None:
        response_message = config.get_message(
            bot_config, "admin_credits_updated",
            user_id=target_user_id, new_balance=new_balance
        )
    else:
        response_message = config.get_message(bot_config, "admin_user_not_found", user_id=target_user_id)
        
    await message.answer(response_message)

async def send_daily_stats(bot: Bot, bot_config: dict, db_name: str):
    stats = database.get_daily_stats(db_name)
    report_text = config.get_message(bot_config, "daily_stats_report_template", **stats)
    admin_ids = config.get_admin_ids()

    for admin_id in admin_ids:
        try:
            await bot.send_message(admin_id, report_text)
        except Exception as e:
            pass

async def main():
    bot_config = config.load_config()
    if not bot_config:
        return
    db_name = bot_config.get("db_name", "bot_database.db")

    database.init_db(db_name)

    bot = Bot(token=bot_config["telegram_bot_token"])
    dp = Dispatcher(bot_config=bot_config, db_name=db_name)

    dp.message.register(handle_start, CommandStart())
    dp.message.register(handle_info_commands, Command("info", "help", "instruct"))
    dp.message.register(handle_stats, Command("stats"))
    dp.message.register(handle_photo, F.photo | (F.document & F.document.mime_type.startswith("image/")))
    
    dp.message.register(handle_admin_balance_commands, Command("add", "set"), IsAdmin())
    
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(send_daily_stats, 'cron', hour=6, minute=0, args=[bot, bot_config, db_name])
    scheduler.start()

    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())