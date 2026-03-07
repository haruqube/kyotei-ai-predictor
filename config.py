"""競艇予想AI設定"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── パス ──
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = DATA_DIR / "cache"
DB_PATH = BASE_DIR / "db" / "kyotei.db"
RESULTS_DIR = BASE_DIR / "results"
TEMPLATES_DIR = BASE_DIR / "publishing" / "templates"

# ── X (Twitter) ──
X_API_KEY = os.getenv("X_API_KEY", "")
X_API_SECRET = os.getenv("X_API_SECRET", "")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.getenv("X_ACCESS_SECRET", "")

# ── スクレイピング ──
SCRAPE_DELAY = 1.5  # 秒
BOATRACE_BASE_URL = "https://www.boatrace.jp"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ── モデル ──
TRAIN_YEARS = [2023, 2024]
TEST_YEARS = [2025]
LGBM_PARAMS = {
    "objective": "lambdarank",
    "metric": "ndcg",
    "ndcg_eval_at": [1, 3],
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 20,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 5,
    "verbose": -1,
}
LGBM_NUM_BOOST_ROUND = 500
LGBM_EARLY_STOPPING_ROUNDS = 50

# ── 記事 ──
NOTE_PRICE_NORMAL = 200
NOTE_PRICE_SG = 500

# ── 競艇場コード (boatrace.jp: 2桁) ──
VENUE_CODES = {
    "01": "桐生", "02": "戸田", "03": "江戸川", "04": "平和島",
    "05": "多摩川", "06": "浜名湖", "07": "蒲郡", "08": "常滑",
    "09": "津", "10": "三国", "11": "びわこ", "12": "住之江",
    "13": "尼崎", "14": "鳴門", "15": "丸亀", "16": "児島",
    "17": "宮島", "18": "徳山", "19": "下関", "20": "若松",
    "21": "芦屋", "22": "福岡", "23": "唐津", "24": "大村",
}

# ── レースグレード ──
GRADE_MAP = {
    "SG": 1, "G1": 2, "G2": 3, "G3": 4,
    "一般": 5,
}

# ── 選手級別 ──
CLASS_MAP = {
    "A1": 1, "A2": 2, "B1": 3, "B2": 4,
}

# ── コース別1着率の全国平均 (参考値) ──
COURSE_WIN_RATE_AVG = {
    1: 0.55, 2: 0.15, 3: 0.12,
    4: 0.10, 5: 0.05, 6: 0.03,
}

# ── インコース有利な競艇場 (1コース1着率が高い場) ──
INNER_COURSE_STRONG_VENUES = [
    "大村", "徳山", "芦屋", "下関", "津",
]
