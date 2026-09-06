import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_signals (
            id SERIAL PRIMARY KEY,
            trade_id VARCHAR(100) UNIQUE NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            headline TEXT NOT NULL,
            news_vector vector(768),
            tech_vector vector(3),
            atr_val NUMERIC(10,5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_performance (
            id SERIAL PRIMARY KEY,
            trade_id VARCHAR(100) REFERENCES trade_signals(trade_id) ON DELETE CASCADE,
            close_reason VARCHAR(30),
            duration_minutes NUMERIC(10,2),
            equity_peak_drawdown NUMERIC(5,2),
            pnl_amount NUMERIC(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("✅ تم إنشاء قاعدة البيانات والجداول بنجاح على Neon PostgreSQL!")