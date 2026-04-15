-- ============================================================
-- kyotei-ai-predictor: Supabase 初期スキーマ
-- SQLite 5テーブル → PostgreSQL + RLS + prediction_detail ビュー
-- ============================================================

-- ── レース情報 ──
CREATE TABLE IF NOT EXISTS races (
    race_id TEXT PRIMARY KEY,
    date DATE NOT NULL,
    venue_code TEXT NOT NULL,
    venue_name TEXT,
    race_number INTEGER NOT NULL,
    race_name TEXT,
    grade TEXT,
    race_type TEXT,
    distance INTEGER DEFAULT 1800,
    weather TEXT,
    wind_direction TEXT,
    wind_speed DOUBLE PRECISION,
    wave_height DOUBLE PRECISION,
    is_night BOOLEAN DEFAULT false,
    racer_count INTEGER DEFAULT 6,
    start_time TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_races_date ON races(date);
CREATE INDEX IF NOT EXISTS idx_races_date_id ON races(date, race_id);
CREATE INDEX IF NOT EXISTS idx_races_venue ON races(venue_code);

-- ── 選手マスタ ──
CREATE TABLE IF NOT EXISTS racers (
    racer_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    branch TEXT,
    birth_year INTEGER,
    class TEXT,
    win_rate_national DOUBLE PRECISION,
    win_rate_2nd_national DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- ── レース結果 ──
CREATE TABLE IF NOT EXISTS race_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id TEXT NOT NULL REFERENCES races(race_id),
    racer_id TEXT NOT NULL REFERENCES racers(racer_id),
    course INTEGER NOT NULL,
    finish_position INTEGER,
    racer_name TEXT,
    class TEXT,
    branch TEXT,
    national_win_rate DOUBLE PRECISION,
    national_2nd_rate DOUBLE PRECISION,
    local_win_rate DOUBLE PRECISION,
    local_2nd_rate DOUBLE PRECISION,
    motor_no INTEGER,
    motor_2nd_rate DOUBLE PRECISION,
    boat_no INTEGER,
    boat_2nd_rate DOUBLE PRECISION,
    exhibition_time DOUBLE PRECISION,
    start_timing DOUBLE PRECISION,
    finish_time TEXT,
    odds DOUBLE PRECISION,
    popularity INTEGER,
    UNIQUE(race_id, racer_id)
);

CREATE INDEX IF NOT EXISTS idx_results_race ON race_results(race_id);
CREATE INDEX IF NOT EXISTS idx_results_racer ON race_results(racer_id);
CREATE INDEX IF NOT EXISTS idx_results_course ON race_results(course);

-- ── 出走表（レース前） ──
CREATE TABLE IF NOT EXISTS entries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id TEXT NOT NULL REFERENCES races(race_id),
    racer_id TEXT NOT NULL,
    course INTEGER NOT NULL,
    racer_name TEXT,
    class TEXT,
    branch TEXT,
    national_win_rate DOUBLE PRECISION,
    national_2nd_rate DOUBLE PRECISION,
    local_win_rate DOUBLE PRECISION,
    local_2nd_rate DOUBLE PRECISION,
    motor_no INTEGER,
    motor_2nd_rate DOUBLE PRECISION,
    boat_no INTEGER,
    boat_2nd_rate DOUBLE PRECISION,
    exhibition_time DOUBLE PRECISION,
    start_timing DOUBLE PRECISION,
    odds DOUBLE PRECISION,
    popularity INTEGER,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(race_id, racer_id)
);

CREATE INDEX IF NOT EXISTS idx_entries_race ON entries(race_id);
CREATE INDEX IF NOT EXISTS idx_entries_race_racer ON entries(race_id, racer_id);

-- ── 予測 ──
CREATE TABLE IF NOT EXISTS predictions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id TEXT NOT NULL REFERENCES races(race_id),
    racer_id TEXT NOT NULL,
    predicted_score DOUBLE PRECISION,
    predicted_rank INTEGER,
    mark TEXT,
    confidence DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(race_id, racer_id)
);

CREATE INDEX IF NOT EXISTS idx_predictions_race ON predictions(race_id);

-- ── 予測結果 ──
CREATE TABLE IF NOT EXISTS prediction_results (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    race_id TEXT NOT NULL REFERENCES races(race_id),
    predicted_top1 TEXT,
    predicted_top3 TEXT,
    actual_top1 TEXT,
    actual_top3 TEXT,
    top1_hit INTEGER DEFAULT 0,
    top3_hit INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(race_id)
);

-- ============================================================
-- RLS (Row Level Security)
-- 全テーブル: anon ユーザーに SELECT のみ許可
-- ============================================================

ALTER TABLE races ENABLE ROW LEVEL SECURITY;
ALTER TABLE racers ENABLE ROW LEVEL SECURITY;
ALTER TABLE race_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE prediction_results ENABLE ROW LEVEL SECURITY;

CREATE POLICY "anon_select_races" ON races FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_racers" ON racers FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_race_results" ON race_results FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_entries" ON entries FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_predictions" ON predictions FOR SELECT TO anon USING (true);
CREATE POLICY "anon_select_prediction_results" ON prediction_results FOR SELECT TO anon USING (true);

-- service_role は全操作可能（デフォルト）

-- ============================================================
-- prediction_detail ビュー
-- フロントエンドから1クエリで予測詳細を取得
-- ============================================================

CREATE OR REPLACE VIEW prediction_detail
WITH (security_invoker = on) AS
SELECT
    p.race_id,
    r.date AS race_date,
    r.venue_name,
    r.venue_code,
    r.race_number,
    r.race_name,
    r.grade,
    r.distance,
    r.weather,
    r.wind_speed,
    r.wave_height,
    r.is_night,
    r.racer_count,
    r.start_time,
    p.racer_id,
    COALESCE(e.racer_name, rc.name) AS racer_name,
    COALESCE(e.class, rc.class) AS racer_class,
    rc.branch,
    e.course,
    e.national_win_rate,
    e.national_2nd_rate,
    e.local_win_rate,
    e.local_2nd_rate,
    e.motor_no,
    e.motor_2nd_rate,
    e.boat_no,
    e.boat_2nd_rate,
    e.exhibition_time,
    e.start_timing,
    p.predicted_score,
    p.predicted_rank,
    p.mark,
    p.confidence,
    rr.finish_position,
    rr.finish_time AS result_finish_time,
    rr.odds AS result_odds
FROM predictions p
JOIN races r ON r.race_id = p.race_id
LEFT JOIN racers rc ON rc.racer_id = p.racer_id
LEFT JOIN entries e ON e.race_id = p.race_id AND e.racer_id = p.racer_id
LEFT JOIN race_results rr ON rr.race_id = p.race_id AND rr.racer_id = p.racer_id
ORDER BY r.date DESC, r.race_number, p.predicted_rank;
