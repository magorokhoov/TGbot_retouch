import json
import redis
from typing import Dict, Any, Optional

def enqueue_task(redis_host: str, redis_port: int, queue_name: str, task_data: Dict[str, Any]) -> bool:
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        task_json = json.dumps(task_data)
        r.lpush(queue_name, task_json)
        return True
    except redis.exceptions.ConnectionError as e:
        return False
    except Exception as e:
        return False

def dequeue_task(redis_host: str, redis_port: int, queue_name: str, timeout: int = 0) -> Optional[Dict[str, Any]]:
    try:
        r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        result = r.brpop(queue_name, timeout=timeout)
        if result:
            _, task_json = result
            task_data = json.loads(task_json)
            return task_data
        else:
            return None
    except redis.exceptions.ConnectionError as e:
        return None
    except json.JSONDecodeError as e:
        return None
    except Exception as e:
        return None