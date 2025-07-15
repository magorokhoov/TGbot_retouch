import json
import os
from typing import Dict, Any, List, Optional

CONFIG_FILE = 'config.json'
ADMINS_FILE = 'admins.txt'

def load_config(file_path: str = CONFIG_FILE) -> Optional[Dict[str, Any]]:
    """Загружает конфигурацию из JSON-файла."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return None

def get_message(config: Dict[str, Any], key: str, **kwargs) -> str:
    """Возвращает текст сообщения по ключу, форматируя его."""
    message_template = config.get("messages", {}).get(key, f"MESSAGE_KEY_NOT_FOUND: {key}")
    try:
        formatted_message = message_template.format(**kwargs)
        return formatted_message
    except KeyError as e:
        return message_template

def get_admin_ids(file_path: str = ADMINS_FILE) -> List[int]:
    """Читает и возвращает список ID администраторов из файла."""
    if not os.path.exists(file_path):
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    admin_ids = []
    for line in lines:
        try:
            admin_id = int(line.strip())
            if admin_id:
                admin_ids.append(admin_id)
        except ValueError:
            pass
    return admin_ids

def add_admin_id(admin_id: int, file_path: str = ADMINS_FILE) -> bool:
    """Добавляет ID администратора в файл."""
    try:
        current_admins = get_admin_ids(file_path)
        if admin_id in current_admins:
            return True
        
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(f"\n{admin_id}")
        return True
    except IOError as e:
        return False

def remove_admin_id(admin_id: int, file_path: str = ADMINS_FILE) -> bool:
    """Удаляет ID администратора из файла."""
    try:
        current_admins = get_admin_ids(file_path)
        if admin_id not in current_admins:
            return True
            
        current_admins.remove(admin_id)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            for id_ in current_admins:
                f.write(f"{id_}\n")
        return True
    except IOError as e:
        return False