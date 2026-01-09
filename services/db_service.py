import sqlite3
from config import SCHEDULE_DB

def get_connection():
    return sqlite3.connect(SCHEDULE_DB)