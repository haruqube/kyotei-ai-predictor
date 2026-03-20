"""コース・レース条件特徴量

競艇はコース(枠番)が勝敗に最も大きく影響する。
1コースの全国平均1着率は約55%。
"""

import sqlite3
from features.base import BaseFeatureBuilder
from db.schema import get_connection
from config import GRADE_MAP, CLASS_MAP, COURSE_WIN_RATE_AVG


def _row_get(row, key, default=None):
    """sqlite3.Row用の安全なgetヘルパー"""
    try:
        val = row[key]
        return val if val is not None else default
    except (IndexError, KeyError):
        return default


class CourseFeatureBuilder(BaseFeatureBuilder):
    """コース位置・レース条件・装備から特徴量を生成"""

    @property
    def feature_names(self) -> list[str]:
        return [
            "course", "is_inner_course",
            "course_avg_win_rate",
            "racer_course_win_rate", "racer_course_top3_rate",
            "racer_course_count",
            "grade_code", "class_code",
            "motor_2nd_rate", "boat_2nd_rate",
            "exhibition_time", "exhibition_rank",
            "start_timing",
            "odds", "popularity",
            "wind_speed", "wave_height",
            "is_night",
            "is_headwind",
            "month",
        ]

    def build(self, race_id: str, racer_id: str, race_date: str) -> dict:
        conn = get_connection()
        try:
            return self._build(conn, race_id, racer_id, race_date)
        finally:
            conn.close()

    def _build(self, conn: sqlite3.Connection, race_id: str, racer_id: str, race_date: str) -> dict:
        feats = {name: None for name in self.feature_names}

        # レース情報
        race = conn.execute(
            "SELECT * FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()

        # 出走情報
        entry = conn.execute(
            "SELECT * FROM entries WHERE race_id = ? AND racer_id = ?",
            (race_id, racer_id)
        ).fetchone()
        if not entry:
            entry = conn.execute(
                "SELECT * FROM race_results WHERE race_id = ? AND racer_id = ?",
                (race_id, racer_id)
            ).fetchone()

        if not entry:
            return feats

        course = entry["course"]
        feats["course"] = course
        feats["is_inner_course"] = 1 if course <= 2 else 0
        feats["course_avg_win_rate"] = COURSE_WIN_RATE_AVG.get(course, 0.05)

        # 級別
        racer_class = _row_get(entry, "class", "")
        feats["class_code"] = CLASS_MAP.get(racer_class, 4)

        # モーター・ボート
        feats["motor_2nd_rate"] = _row_get(entry, "motor_2nd_rate")
        feats["boat_2nd_rate"] = _row_get(entry, "boat_2nd_rate")
        feats["exhibition_time"] = _row_get(entry, "exhibition_time")
        feats["start_timing"] = _row_get(entry, "start_timing")
        feats["odds"] = _row_get(entry, "odds")
        feats["popularity"] = _row_get(entry, "popularity")

        # レース条件
        if race:
            feats["grade_code"] = GRADE_MAP.get(race["grade"], 5) if race["grade"] else 5
            feats["wind_speed"] = _row_get(race, "wind_speed")
            feats["wave_height"] = _row_get(race, "wave_height")
            feats["is_night"] = _row_get(race, "is_night", 0)

            # 向かい風判定（1コースに不利）
            wind_dir = _row_get(race, "wind_direction", "")
            feats["is_headwind"] = 1 if wind_dir in ("向", "北", "北西", "北東") else 0

            if race["date"]:
                try:
                    feats["month"] = int(race["date"].split("-")[1])
                except (IndexError, ValueError):
                    pass

        # 展示タイム順位
        if feats["exhibition_time"] and race:
            all_entries = conn.execute("""
                SELECT exhibition_time FROM entries
                WHERE race_id = ? AND exhibition_time IS NOT NULL
                UNION
                SELECT exhibition_time FROM race_results
                WHERE race_id = ? AND exhibition_time IS NOT NULL
            """, (race_id, race_id)).fetchall()
            if all_entries:
                times = sorted(set(r["exhibition_time"] for r in all_entries))
                try:
                    feats["exhibition_rank"] = times.index(feats["exhibition_time"]) + 1
                except ValueError:
                    feats["exhibition_rank"] = 3

        # 同コースでの過去成績
        past = conn.execute("""
            SELECT rr.finish_position
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.racer_id = ? AND rr.course = ? AND r.date < ?
            ORDER BY r.date DESC LIMIT 30
        """, (racer_id, course, race_date)).fetchall()

        if past:
            c_pos = [r["finish_position"] for r in past if r["finish_position"]]
            feats["racer_course_count"] = len(c_pos)
            if c_pos:
                feats["racer_course_win_rate"] = sum(1 for p in c_pos if p == 1) / len(c_pos)
                feats["racer_course_top3_rate"] = sum(1 for p in c_pos if p <= 3) / len(c_pos)

        return feats
