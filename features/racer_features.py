"""選手成績ベース特徴量"""

import sqlite3
from features.base import BaseFeatureBuilder
from db.schema import get_connection


class RacerFeatureBuilder(BaseFeatureBuilder):
    """選手の過去成績から特徴量を生成"""

    @property
    def feature_names(self) -> list[str]:
        return [
            "avg_finish_5", "avg_finish_10", "avg_finish_all",
            "win_rate", "top2_rate", "top3_rate",
            "race_count", "win_count",
            "days_since_last_race",
            "national_win_rate", "national_2nd_rate",
            "local_win_rate", "local_2nd_rate",
            "venue_win_rate", "venue_top3_rate",
            "avg_start_timing_5",
            "flying_count", "late_start_count",
        ]

    def build(self, race_id: str, racer_id: str, race_date: str) -> dict:
        conn = get_connection()
        try:
            return self._build(conn, race_id, racer_id, race_date)
        finally:
            conn.close()

    def _build(self, conn: sqlite3.Connection, race_id: str, racer_id: str, race_date: str) -> dict:
        rows = conn.execute("""
            SELECT rr.*, r.venue_code, r.date as race_date
            FROM race_results rr
            JOIN races r ON rr.race_id = r.race_id
            WHERE rr.racer_id = ? AND r.date < ?
            ORDER BY r.date DESC
        """, (racer_id, race_date)).fetchall()

        feats = {name: None for name in self.feature_names}
        if not rows:
            feats["race_count"] = 0
            return feats

        positions = [r["finish_position"] for r in rows if r["finish_position"]]
        st_vals = [r["start_timing"] for r in rows if r["start_timing"] is not None]

        feats["race_count"] = len(rows)
        feats["win_count"] = sum(1 for p in positions if p == 1)

        if positions:
            feats["avg_finish_all"] = sum(positions) / len(positions)
            feats["avg_finish_5"] = sum(positions[:5]) / min(5, len(positions))
            feats["avg_finish_10"] = sum(positions[:10]) / min(10, len(positions))
            feats["win_rate"] = feats["win_count"] / len(positions)
            feats["top2_rate"] = sum(1 for p in positions if p <= 2) / len(positions)
            feats["top3_rate"] = sum(1 for p in positions if p <= 3) / len(positions)

        if st_vals:
            feats["avg_start_timing_5"] = sum(st_vals[:5]) / min(5, len(st_vals))
            feats["flying_count"] = sum(1 for s in st_vals if s < 0)
            feats["late_start_count"] = sum(1 for s in st_vals if s > 0.20)

        # 前走からの経過日数
        if rows[0]["race_date"]:
            from datetime import datetime
            try:
                last_dt = datetime.strptime(rows[0]["race_date"], "%Y-%m-%d")
                curr_dt = datetime.strptime(race_date, "%Y-%m-%d")
                feats["days_since_last_race"] = (curr_dt - last_dt).days
            except ValueError:
                pass

        # 当レースのエントリ情報から全国/当地勝率を取得
        entry = conn.execute(
            "SELECT * FROM entries WHERE race_id = ? AND racer_id = ?",
            (race_id, racer_id)
        ).fetchone()
        if not entry:
            entry = conn.execute(
                "SELECT * FROM race_results WHERE race_id = ? AND racer_id = ?",
                (race_id, racer_id)
            ).fetchone()

        if entry:
            feats["national_win_rate"] = entry["national_win_rate"]
            feats["national_2nd_rate"] = entry["national_2nd_rate"]
            feats["local_win_rate"] = entry["local_win_rate"]
            feats["local_2nd_rate"] = entry["local_2nd_rate"]

        # 同会場での成績
        race_row = conn.execute(
            "SELECT venue_code FROM races WHERE race_id = ?", (race_id,)
        ).fetchone()
        if race_row:
            venue_rows = [r for r in rows
                          if r["venue_code"] == race_row["venue_code"]
                          and r["finish_position"]]
            if venue_rows:
                v_pos = [r["finish_position"] for r in venue_rows]
                feats["venue_win_rate"] = sum(1 for p in v_pos if p == 1) / len(v_pos)
                feats["venue_top3_rate"] = sum(1 for p in v_pos if p <= 3) / len(v_pos)

        return feats
