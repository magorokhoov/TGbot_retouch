import sqlite3
from typing import Optional, Tuple, Dict, List, Any


def _get_connection(db_name: str) -> Optional[sqlite3.Connection]:
    try:
        conn = sqlite3.connect(db_name, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except sqlite3.Error as e:
        return None

def init_db(db_name: str):
    with _get_connection(db_name) as conn:
        if conn is None:
            return
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    balance INTEGER NOT NULL,
                    total_processed INTEGER NOT NULL DEFAULT 0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS processing_history (
                    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    original_photo_path TEXT,
                    processed_photo_path TEXT,
                    processing_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            """)
            conn.commit()
        except sqlite3.Error as e:
            pass

def add_or_get_user(db_name: str, user_id: int, start_balance: int) -> bool:
    with _get_connection(db_name) as conn:
        if conn is None: return False
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if cursor.fetchone():
                return False
            else:
                cursor.execute("INSERT INTO users (user_id, balance) VALUES (?, ?)", (user_id, start_balance))
                conn.commit()
                return True
        except sqlite3.Error as e:
            return False

def get_user_balance(db_name: str, user_id: int) -> Optional[int]:
    with _get_connection(db_name) as conn:
        if conn is None: return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            if result:
                balance = result['balance']
                return balance
            else:
                return None
        except sqlite3.Error as e:
            return None

def update_user_balance(db_name: str, user_id: int, amount: int, set_exact: bool = False) -> Optional[int]:
    with _get_connection(db_name) as conn:
        if conn is None: return None
        try:
            cursor = conn.cursor()
            if set_exact:
                sql = "UPDATE users SET balance = ? WHERE user_id = ?"
                params = (amount, user_id)
            else:
                sql = "UPDATE users SET balance = balance + ? WHERE user_id = ?"
                params = (amount, user_id)
            
            cursor.execute(sql, params)
            if cursor.rowcount == 0:
                return None

            cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
            new_balance = cursor.fetchone()['balance']
            conn.commit()
            return new_balance
        except sqlite3.Error as e:
            return None

def spend_credit(db_name: str, user_id: int) -> bool:
    with _get_connection(db_name) as conn:
        if conn is None: return False
        try:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = balance - 1, total_processed = total_processed + 1 WHERE user_id = ? AND balance > 0", (user_id,))
            if cursor.rowcount > 0:
                conn.commit()
                return True
            else:
                return False
        except sqlite3.Error as e:
            return False

def add_processing_history(db_name: str, user_id: int, original_path: str, processed_path: str) -> bool:
    with _get_connection(db_name) as conn:
        if conn is None: return False
        try:
            conn.execute(
                "INSERT INTO processing_history (user_id, original_photo_path, processed_photo_path) VALUES (?, ?, ?)",
                (user_id, original_path, processed_path)
            )
            conn.commit()
            return True
        except sqlite3.Error as e:
            return False

def get_user_stats(db_name: str, user_id: int) -> Optional[Dict[str, int]]:
    with _get_connection(db_name) as conn:
        if conn is None: return None
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT balance, total_processed FROM users WHERE user_id = ?", (user_id,))
            result = cursor.fetchone()
            if result:
                stats = {'balance': result['balance'], 'total_processed': result['total_processed']}
                return stats
            return None
        except sqlite3.Error as e:
            return None

def get_daily_stats(db_name: str) -> Dict[str, Any]:
    stats = {
        'new_users': 0,
        'processed_photos': 0,
        'active_users': 0,
        'total_users': 0,
        'total_processed_ever': 0
    }
    with _get_connection(db_name) as conn:
        if conn is None: return stats
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(user_id) FROM users WHERE registration_date >= datetime('now', '-1 day')")
            stats['new_users'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(history_id) FROM processing_history WHERE processing_date >= datetime('now', '-1 day')")
            stats['processed_photos'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM processing_history WHERE processing_date >= datetime('now', '-1 day')")
            stats['active_users'] = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(user_id) FROM users")
            stats['total_users'] = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(total_processed) FROM users")
            total_processed = cursor.fetchone()[0]
            stats['total_processed_ever'] = total_processed if total_processed else 0
            
        except sqlite3.Error as e:
            pass
    return stats