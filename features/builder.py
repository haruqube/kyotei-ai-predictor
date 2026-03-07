"""全特徴量を集約するビルダー"""

import pandas as pd
from db.schema import get_connection
from features.racer_features import RacerFeatureBuilder
from features.course_features import CourseFeatureBuilder


class FeatureBuilder:
    """全特徴量ビルダーを統合し、レース単位のDataFrameを生成"""

    def __init__(self):
        self.builders = [
            RacerFeatureBuilder(),
            CourseFeatureBuilder(),
        ]

    @property
    def feature_names(self) -> list[str]:
        names = []
        for b in self.builders:
            names.extend(b.feature_names)
        return names

    def build_race_features(self, race_id: str, race_date: str) -> pd.DataFrame:
        """1レース分の全出走選手の特徴量DataFrameを作成"""
        conn = get_connection()
        try:
            racers = conn.execute(
                "SELECT DISTINCT racer_id FROM entries WHERE race_id = ?",
                (race_id,)
            ).fetchall()

            if not racers:
                racers = conn.execute(
                    "SELECT DISTINCT racer_id FROM race_results WHERE race_id = ?",
                    (race_id,)
                ).fetchall()

            if not racers:
                return pd.DataFrame()

            rows = []
            for r in racers:
                racer_id = r["racer_id"]
                if not racer_id:
                    continue
                feat_row = {"race_id": race_id, "racer_id": racer_id}
                for builder in self.builders:
                    feats = builder.build(race_id, racer_id, race_date)
                    feat_row.update(feats)
                rows.append(feat_row)

            return pd.DataFrame(rows)
        finally:
            conn.close()

    def build_dataset(self, year_start: int, year_end: int) -> pd.DataFrame:
        """指定年範囲のレースすべてから学習データセットを作成"""
        conn = get_connection()
        races = conn.execute("""
            SELECT race_id, date FROM races
            WHERE date >= ? AND date <= ?
            ORDER BY date
        """, (f"{year_start}-01-01", f"{year_end}-12-31")).fetchall()
        conn.close()

        all_dfs = []
        for race in races:
            race_id = race["race_id"]
            race_date = race["date"]
            df = self.build_race_features(race_id, race_date)
            if not df.empty:
                conn2 = get_connection()
                results = conn2.execute(
                    "SELECT racer_id, finish_position FROM race_results WHERE race_id = ?",
                    (race_id,)
                ).fetchall()
                conn2.close()

                finish_map = {r["racer_id"]: r["finish_position"] for r in results}
                df["finish_position"] = df["racer_id"].map(finish_map)
                df["race_date"] = race_date
                all_dfs.append(df)

        if not all_dfs:
            return pd.DataFrame()

        return pd.concat(all_dfs, ignore_index=True)
