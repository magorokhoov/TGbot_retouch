import os
import time
import uuid
from typing import Dict, Any

from aiogram import Bot
from aiogram.types import FSInputFile

import config
import database
import image_utils
import queue_client

async def send_system_alert(bot: Bot, bot_config: Dict, alert_type: str, message: str, context: str):
    admin_ids = config.get_admin_ids()
    alert_text = config.get_message(
        bot_config, "system_alert_template",
        alert_type=alert_type, message=message, context=context
    )
    for admin_id in admin_ids:
        await bot.send_message(admin_id, alert_text)

async def process_task(task_data: Dict[str, Any], bot: Bot, bot_config: Dict):
    user_id = task_data.get("user_id")
    original_path = task_data.get("original_path")
    db_name = bot_config.get("db_name")

    file_name = os.path.basename(original_path)
    name, ext = os.path.splitext(file_name)
    processed_filename = f"{name}_processed_{uuid.uuid4().hex[:8]}{ext}"
    processed_path = os.path.join(os.path.dirname(original_path), processed_filename)

    result_path = image_utils.apply_blur(original_path, processed_path)

    if result_path:
        try:
            caption = config.get_message(bot_config, "processing_complete")
            photo = FSInputFile(result_path)
            await bot.send_photo(user_id, photo, caption=caption)
            database.add_processing_history(db_name, user_id, original_path, result_path)
        except Exception as e:
            await send_system_alert(bot, bot_config, "SEND_ERROR", str(e), f"user_id: {user_id}")
    else:
        database.update_user_balance(db_name, user_id, 1, set_exact=False)
        await send_system_alert(bot, bot_config, "PROCESSING_ERROR", f"File: {original_path}", f"user_id: {user_id}")

async def main_worker_loop():
    bot_config = config.load_config()
    if not bot_config:
        return

    bot = Bot(token=bot_config["telegram_bot_token"])
    redis_host = bot_config["redis_host"]
    redis_port = bot_config["redis_port"]
    queue_name = bot_config["redis_queue_name"]
    
    while True:
        task = queue_client.dequeue_task(redis_host, redis_port, queue_name, timeout=0)

        if task:
            try:
                await process_task(task, bot, bot_config)
            except Exception as e:
                await send_system_alert(bot, bot_config, "WORKER_CRASH", str(e), f"task: {task}")
        else:
            time.sleep(1)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main_worker_loop())