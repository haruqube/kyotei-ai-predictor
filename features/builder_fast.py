"""高速バッチ特徴量ビルダー

個別クエリではなく、バッチSQLで全データを一括取得してDataFrameに変換する。
110Kレース × 6選手 = 660K行を数分で処理可能。
"""

import pandas as pd
import numpy as np
from db.schema import get_connection
from config import GRADE_MAP, CLASS_MAP, COURSE_WIN_RATE_AVG


# 全特徴量名
FEATURE_NAMES = [
    # 選手成績系 (18)
    "avg_finish_5", "avg_finish_10", "avg_finish_all",
    "win_rate", "top2_rate", "top3_rate",
    "race_count", "win_count",
    "days_since_last_race",
    "national_win_rate", "national_2nd_rate",
    "local_win_rate", "local_2nd_rate",
    "venue_win_rate", "venue_top3_rate",
    "avg_start_timing_5",
    "flying_count", "late_start_count",
    # コース・条件系 (19)
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


def build_dataset_fast(year_start: int, year_end: int, verbose: bool = True) -> pd.DataFrame:
    """バッチSQLで高速に学習データセットを構築"""
    conn = get_connection()

    date_start = f"{year_start}-01-01"
    date_end = f"{year_end}-12-31"

    if verbose:
        print(f"Loading races {date_start} ~ {date_end}...")

    # ── 1. 対象レースの結果を一括取得 ──
    results_df = pd.read_sql_query("""
        SELECT rr.race_id, rr.racer_id, rr.course, rr.finish_position,
               rr.class, rr.national_win_rate, rr.national_2nd_rate,
               rr.local_win_rate, rr.local_2nd_rate,
               rr.motor_2nd_rate, rr.boat_2nd_rate,
               rr.exhibition_time, rr.start_timing,
               rr.odds, rr.popularity,
               r.date as race_date, r.venue_code, r.grade,
               r.wind_direction, r.wind_speed, r.wave_height,
               r.is_night, r.race_number
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        WHERE r.date >= ? AND r.date <= ?
        ORDER BY r.date, rr.race_id, rr.course
    """, conn, params=(date_start, date_end))

    if verbose:
        print(f"  Target results: {len(results_df)} rows")

    if results_df.empty:
        conn.close()
        return pd.DataFrame()

    # ── 2. 全期間の結果も取得（過去成績計算用） ──
    all_results_df = pd.read_sql_query("""
        SELECT rr.race_id, rr.racer_id, rr.course, rr.finish_position,
               rr.start_timing, r.date as race_date, r.venue_code
        FROM race_results rr
        JOIN races r ON rr.race_id = r.race_id
        ORDER BY r.date
    """, conn)

    conn.close()

    if verbose:
        print(f"  All results (for history): {len(all_results_df)} rows")
        print("Computing features...")

    # ── 3. 基本カラム ──
    df = results_df.copy()
    df["month"] = pd.to_datetime(df["race_date"]).dt.month

    # コース特徴量
    df["is_inner_course"] = (df["course"] <= 2).astype(int)
    df["course_avg_win_rate"] = df["course"].map(COURSE_WIN_RATE_AVG).fillna(0.05)

    # グレード・級別コード
    df["grade_code"] = df["grade"].map(GRADE_MAP).fillna(5).astype(int)
    df["class_code"] = df["class"].map(CLASS_MAP).fillna(4).astype(int)

    # 向かい風判定
    headwind_dirs = {"向", "北", "北西", "北東"}
    df["is_headwind"] = df["wind_direction"].isin(headwind_dirs).astype(int)

    # ── 4. 展示タイム順位（レース内ランク） ──
    df["exhibition_rank"] = df.groupby("race_id")["exhibition_time"].rank(method="min")

    # ── 5. 選手の過去成績（累積統計） ──
    # 日付でソートして、各選手の過去成績を計算
    hist = all_results_df.copy()
    hist = hist.sort_values(["racer_id", "race_date"]).reset_index(drop=True)

    # 選手×日付ごとの累積統計を計算
    racer_stats = _compute_racer_cumulative_stats(hist)
    # 選手×コース×日付ごとの累積統計
    racer_course_stats = _compute_racer_course_stats(hist)
    # 選手×会場×日付ごとの累積統計
    racer_venue_stats = _compute_racer_venue_stats(hist)

    if verbose:
        print("  Merging racer stats...")

    # マージ
    df = df.merge(racer_stats, on=["racer_id", "race_date"], how="left")
    df = df.merge(racer_course_stats, on=["racer_id", "course", "race_date"], how="left")
    df = df.merge(racer_venue_stats, on=["racer_id", "venue_code", "race_date"], how="left")

    # 欠損値処理
    df["race_count"] = df["race_count"].fillna(0)
    df["win_count"] = df["win_count"].fillna(0)

    if verbose:
        print(f"  Dataset: {len(df)} rows, {df['race_id'].nunique()} races")

    return df


def _compute_racer_cumulative_stats(hist: pd.DataFrame) -> pd.DataFrame:
    """選手ごとの累積統計（各日付時点の過去成績）"""
    hist = hist.sort_values(["racer_id", "race_date"]).copy()
    hist["pos"] = hist["finish_position"]

    # 各選手×各日の「その日より前」の統計を計算
    # アプローチ: 日付ごとにグループ化した累積統計
    records = []
    for racer_id, grp in hist.groupby("racer_id"):
        grp = grp.sort_values("race_date")
        dates = grp["race_date"].values
        positions = grp["pos"].values
        st_vals = grp["start_timing"].values

        # その日が初出場の場合のみ記録（同じ日に複数レースあり）
        unique_dates = sorted(grp["race_date"].unique())

        cum_pos = []
        cum_st = []
        for ud in unique_dates:
            mask_before = dates < ud
            past_pos = positions[mask_before]
            past_pos_valid = past_pos[~pd.isna(past_pos)]
            past_st = st_vals[mask_before]
            past_st_valid = past_st[~pd.isna(past_st)]

            stats = {"racer_id": racer_id, "race_date": ud}
            n = len(past_pos_valid)
            stats["race_count"] = n

            if n > 0:
                stats["win_count"] = int(np.sum(past_pos_valid == 1))
                stats["avg_finish_all"] = np.mean(past_pos_valid)
                stats["avg_finish_5"] = np.mean(past_pos_valid[-5:]) if n >= 1 else np.mean(past_pos_valid)
                stats["avg_finish_10"] = np.mean(past_pos_valid[-10:]) if n >= 1 else np.mean(past_pos_valid)
                stats["win_rate"] = stats["win_count"] / n
                stats["top2_rate"] = np.sum(past_pos_valid <= 2) / n
                stats["top3_rate"] = np.sum(past_pos_valid <= 3) / n
            else:
                stats["win_count"] = 0

            if len(past_st_valid) > 0:
                stats["avg_start_timing_5"] = np.mean(past_st_valid[-5:])
                stats["flying_count"] = int(np.sum(past_st_valid < 0))
                stats["late_start_count"] = int(np.sum(past_st_valid > 0.20))

            # 前走からの経過日数
            past_dates = dates[mask_before]
            if len(past_dates) > 0:
                from datetime import datetime
                try:
                    last = datetime.strptime(str(past_dates[-1]), "%Y-%m-%d")
                    curr = datetime.strptime(str(ud), "%Y-%m-%d")
                    stats["days_since_last_race"] = (curr - last).days
                except (ValueError, TypeError):
                    pass

            records.append(stats)

    return pd.DataFrame(records)


def _compute_racer_course_stats(hist: pd.DataFrame) -> pd.DataFrame:
    """選手×コースごとの累積統計"""
    records = []
    for (racer_id, course), grp in hist.groupby(["racer_id", "course"]):
        grp = grp.sort_values("race_date")
        dates = grp["race_date"].values
        positions = grp["finish_position"].values
        unique_dates = sorted(grp["race_date"].unique())

        for ud in unique_dates:
            mask = dates < ud
            past_pos = positions[mask]
            past_pos_valid = past_pos[~pd.isna(past_pos)]
            n = len(past_pos_valid)

            stats = {"racer_id": racer_id, "course": course, "race_date": ud}
            stats["racer_course_count"] = n
            if n > 0:
                stats["racer_course_win_rate"] = np.sum(past_pos_valid == 1) / n
                stats["racer_course_top3_rate"] = np.sum(past_pos_valid <= 3) / n
            records.append(stats)

    return pd.DataFrame(records)


def _compute_racer_venue_stats(hist: pd.DataFrame) -> pd.DataFrame:
    """選手×会場ごとの累積統計"""
    records = []
    for (racer_id, vc), grp in hist.groupby(["racer_id", "venue_code"]):
        grp = grp.sort_values("race_date")
        dates = grp["race_date"].values
        positions = grp["finish_position"].values
        unique_dates = sorted(grp["race_date"].unique())

        for ud in unique_dates:
            mask = dates < ud
            past_pos = positions[mask]
            past_pos_valid = past_pos[~pd.isna(past_pos)]
            n = len(past_pos_valid)

            stats = {"racer_id": racer_id, "venue_code": vc, "race_date": ud}
            if n > 0:
                stats["venue_win_rate"] = np.sum(past_pos_valid == 1) / n
                stats["venue_top3_rate"] = np.sum(past_pos_valid <= 3) / n
            records.append(stats)

    return pd.DataFrame(records)


if __name__ == "__main__":
    import time
    start = time.time()
    df = build_dataset_fast(2023, 2023)
    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")
    print(f"Shape: {df.shape}")
    print(f"Features: {[c for c in FEATURE_NAMES if c in df.columns]}")
    print(f"Missing features: {[c for c in FEATURE_NAMES if c not in df.columns]}")
