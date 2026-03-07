"""SQLiteスキーマ定義・DB初期化"""

import sqlite3
from pathlib import Path
from config import DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS races (
        race_id TEXT PRIMARY KEY,
        date TEXT NOT NULL,
        venue_code TEXT NOT NULL,
        venue_name TEXT,
        race_number INTEGER NOT NULL,
        race_name TEXT,
        grade TEXT,
        race_type TEXT,
        distance INTEGER DEFAULT 1800,
        weather TEXT,
        wind_direction TEXT,
        wind_speed REAL,
        wave_height REAL,
        is_night INTEGER DEFAULT 0,
        racer_count INTEGER DEFAULT 6,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS racers (
        racer_id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        branch TEXT,
        birth_year INTEGER,
        class TEXT,
        win_rate_national REAL,
        win_rate_2nd_national REAL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS race_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        racer_id TEXT NOT NULL,
        course INTEGER NOT NULL,
        finish_position INTEGER,
        racer_name TEXT,
        class TEXT,
        branch TEXT,
        national_win_rate REAL,
        national_2nd_rate REAL,
        local_win_rate REAL,
        local_2nd_rate REAL,
        motor_no INTEGER,
        motor_2nd_rate REAL,
        boat_no INTEGER,
        boat_2nd_rate REAL,
        exhibition_time REAL,
        start_timing REAL,
        finish_time TEXT,
        odds REAL,
        popularity INTEGER,
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id, racer_id)
    );

    CREATE TABLE IF NOT EXISTS entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        racer_id TEXT NOT NULL,
        course INTEGER NOT NULL,
        racer_name TEXT,
        class TEXT,
        branch TEXT,
        national_win_rate REAL,
        national_2nd_rate REAL,
        local_win_rate REAL,
        local_2nd_rate REAL,
        motor_no INTEGER,
        motor_2nd_rate REAL,
        boat_no INTEGER,
        boat_2nd_rate REAL,
        exhibition_time REAL,
        start_timing REAL,
        odds REAL,
        popularity INTEGER,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id, racer_id)
    );

    CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        racer_id TEXT NOT NULL,
        predicted_score REAL,
        predicted_rank INTEGER,
        mark TEXT,
        confidence REAL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id, racer_id)
    );

    CREATE TABLE IF NOT EXISTS prediction_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        race_id TEXT NOT NULL,
        predicted_top1 TEXT,
        predicted_top3 TEXT,
        actual_top1 TEXT,
        actual_top3 TEXT,
        top1_hit INTEGER DEFAULT 0,
        top3_hit INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (race_id) REFERENCES races(race_id),
        UNIQUE(race_id)
    );

    CREATE INDEX IF NOT EXISTS idx_results_race ON race_results(race_id);
    CREATE INDEX IF NOT EXISTS idx_results_racer ON race_results(racer_id);
    CREATE INDEX IF NOT EXISTS idx_results_course ON race_results(course);
    CREATE INDEX IF NOT EXISTS idx_races_date ON races(date);
    CREATE INDEX IF NOT EXISTS idx_races_venue ON races(venue_code);
    CREATE INDEX IF NOT EXISTS idx_entries_race ON entries(race_id);
    CREATE INDEX IF NOT EXISTS idx_predictions_race ON predictions(race_id);
    """)

    conn.commit()
    conn.close()


def insert_race(conn: sqlite3.Connection, race: dict):
    conn.execute("""
        INSERT OR REPLACE INTO races
        (race_id, date, venue_code, venue_name, race_number, race_name, grade,
         race_type, distance, weather, wind_direction, wind_speed, wave_height,
         is_night, racer_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        race["race_id"], race["date"], race["venue_code"],
        race.get("venue_name"), race["race_number"],
        race.get("race_name"), race.get("grade"),
        race.get("race_type"), race.get("distance", 1800),
        race.get("weather"), race.get("wind_direction"),
        race.get("wind_speed"), race.get("wave_height"),
        race.get("is_night", 0), race.get("racer_count", 6),
    ))


def insert_racer(conn: sqlite3.Connection, racer: dict):
    conn.execute("""
        INSERT OR IGNORE INTO racers
        (racer_id, name, branch, birth_year, class,
         win_rate_national, win_rate_2nd_national)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        racer["racer_id"], racer["name"], racer.get("branch"),
        racer.get("birth_year"), racer.get("class"),
        racer.get("win_rate_national"), racer.get("win_rate_2nd_national"),
    ))


def insert_result(conn: sqlite3.Connection, result: dict):
    conn.execute("""
        INSERT OR REPLACE INTO race_results
        (race_id, racer_id, course, finish_position, racer_name,
         class, branch, national_win_rate, national_2nd_rate,
         local_win_rate, local_2nd_rate,
         motor_no, motor_2nd_rate, boat_no, boat_2nd_rate,
         exhibition_time, start_timing, finish_time, odds, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result["race_id"], result["racer_id"], result["course"],
        result.get("finish_position"), result.get("racer_name"),
        result.get("class"), result.get("branch"),
        result.get("national_win_rate"), result.get("national_2nd_rate"),
        result.get("local_win_rate"), result.get("local_2nd_rate"),
        result.get("motor_no"), result.get("motor_2nd_rate"),
        result.get("boat_no"), result.get("boat_2nd_rate"),
        result.get("exhibition_time"), result.get("start_timing"),
        result.get("finish_time"), result.get("odds"),
        result.get("popularity"),
    ))


def insert_entry(conn: sqlite3.Connection, entry: dict):
    conn.execute("""
        INSERT OR REPLACE INTO entries
        (race_id, racer_id, course, racer_name,
         class, branch, national_win_rate, national_2nd_rate,
         local_win_rate, local_2nd_rate,
         motor_no, motor_2nd_rate, boat_no, boat_2nd_rate,
         exhibition_time, start_timing, odds, popularity)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        entry["race_id"], entry["racer_id"], entry["course"],
        entry.get("racer_name"), entry.get("class"), entry.get("branch"),
        entry.get("national_win_rate"), entry.get("national_2nd_rate"),
        entry.get("local_win_rate"), entry.get("local_2nd_rate"),
        entry.get("motor_no"), entry.get("motor_2nd_rate"),
        entry.get("boat_no"), entry.get("boat_2nd_rate"),
        entry.get("exhibition_time"), entry.get("start_timing"),
        entry.get("odds"), entry.get("popularity"),
    ))


def insert_prediction(conn: sqlite3.Connection, pred: dict):
    conn.execute("""
        INSERT OR REPLACE INTO predictions
        (race_id, racer_id, predicted_score, predicted_rank, mark, confidence)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        pred["race_id"], pred["racer_id"], pred.get("predicted_score"),
        pred.get("predicted_rank"), pred.get("mark"), pred.get("confidence"),
    ))


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
