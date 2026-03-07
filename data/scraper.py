"""boatrace.jpスクレイパー（キャッシュ付き）"""

import json
import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import (
    CACHE_DIR, BOATRACE_BASE_URL,
    REQUEST_HEADERS, SCRAPE_DELAY, VENUE_CODES,
)

logger = logging.getLogger(__name__)


class BoatraceScraper:
    """boatrace.jpからレース情報・結果をスクレイピング

    boatrace.jp URL構造:
      レース一覧: /owpc/pc/race/index?hd=YYYYMMDD
      出走表:     /owpc/pc/race/racelist?rno={R}&jcd={JJ}&hd={YYYYMMDD}
      直前情報:   /owpc/pc/race/beforeinfo?rno={R}&jcd={JJ}&hd={YYYYMMDD}
      結果:       /owpc/pc/race/raceresult?rno={R}&jcd={JJ}&hd={YYYYMMDD}

    race_id形式: YYYYMMDD_JJ_RR (日付_場コード_レース番号)
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get(self, url: str) -> str:
        cache_key = re.sub(r"[^a-zA-Z0-9_\-]", "_", url.replace("https://", ""))
        cache_file = CACHE_DIR / f"{cache_key}.html"

        if cache_file.exists():
            return cache_file.read_text(encoding="utf-8")

        time.sleep(SCRAPE_DELAY)
        resp = self.session.get(url, timeout=30)
        resp.encoding = resp.apparent_encoding or "utf-8"
        html = resp.text
        cache_file.write_text(html, encoding="utf-8")
        logger.debug("Fetched %s", url)
        return html

    def _get_json_cache(self, key: str) -> dict | list | None:
        cache_file = CACHE_DIR / f"{key}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))
        return None

    def _set_json_cache(self, key: str, data):
        cache_file = CACHE_DIR / f"{key}.json"
        cache_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def scrape_race_list(self, date: str) -> list[str]:
        """指定日のレースID一覧を取得 (date: YYYYMMDD)

        boatrace.jpのレース一覧ページから開催場とレース番号を抽出。
        戻り値: ["YYYYMMDD_JJ_RR", ...] 形式のrace_idリスト
        """
        cache_key = f"race_list_{date}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/index?hd={date}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_ids = []

        # 開催場リンクから場コードを抽出
        for link in soup.select("a[href*='raceindex']"):
            href = link.get("href", "")
            jcd_match = re.search(r"jcd=(\d{2})", href)
            if not jcd_match:
                continue
            venue_code = jcd_match.group(1)

            # 各場の全12レースをrace_idとして生成
            for rno in range(1, 13):
                race_id = f"{date}_{venue_code}_{rno:02d}"
                if race_id not in race_ids:
                    race_ids.append(race_id)

        race_ids.sort()
        self._set_json_cache(cache_key, race_ids)
        return race_ids

    def _parse_race_id(self, race_id: str) -> tuple[str, str, int]:
        """race_id → (date, venue_code, race_number)"""
        parts = race_id.split("_")
        return parts[0], parts[1], int(parts[2])

    def scrape_race_result(self, race_id: str) -> dict:
        """レース結果ページをパース"""
        cache_key = f"race_result_{race_id}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        date, jcd, rno = self._parse_race_id(race_id)
        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/raceresult?rno={rno}&jcd={jcd}&hd={date}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_info = self._parse_race_info(soup, race_id, date, jcd, rno)
        results = self._parse_result_table(soup, race_id)
        race_info["results"] = results
        race_info["racer_count"] = len(results)

        self._set_json_cache(cache_key, race_info)
        return race_info

    def scrape_race_entry(self, race_id: str) -> dict:
        """出走表ページをパース（レース前）"""
        cache_key = f"race_entry_{race_id}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        date, jcd, rno = self._parse_race_id(race_id)
        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/racelist?rno={rno}&jcd={jcd}&hd={date}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_info = self._parse_race_info(soup, race_id, date, jcd, rno)
        entries = self._parse_entry_table(soup, race_id)
        race_info["entries"] = entries
        race_info["racer_count"] = len(entries)

        # 直前情報も取得（展示タイム等）
        before_url = f"{BOATRACE_BASE_URL}/owpc/pc/race/beforeinfo?rno={rno}&jcd={jcd}&hd={date}"
        before_html = self._get(before_url)
        before_soup = BeautifulSoup(before_html, "lxml")
        self._merge_before_info(before_soup, entries)

        # 天候情報も直前情報から取得
        weather_info = self._parse_weather(before_soup)
        race_info.update(weather_info)

        self._set_json_cache(cache_key, race_info)
        return race_info

    def _parse_race_info(self, soup: BeautifulSoup, race_id: str,
                         date: str, jcd: str, rno: int) -> dict:
        """レース情報をパース"""
        info = {
            "race_id": race_id,
            "venue_code": jcd,
            "venue_name": VENUE_CODES.get(jcd, jcd),
            "race_number": rno,
        }

        # レース名
        title = soup.select_one(".heading2_titleName, .is-raceTitle")
        info["race_name"] = title.get_text(strip=True) if title else ""

        # グレード
        info["grade"] = None
        grade_el = soup.select_one("[class*='is-SGrade'], [class*='is-G1'], "
                                   "[class*='is-G2'], [class*='is-G3']")
        if grade_el:
            classes = " ".join(grade_el.get("class", []))
            if "SGrade" in classes or "is-SG" in classes:
                info["grade"] = "SG"
            elif "G1" in classes:
                info["grade"] = "G1"
            elif "G2" in classes:
                info["grade"] = "G2"
            elif "G3" in classes:
                info["grade"] = "G3"

        # ナイター判定
        info["is_night"] = 0
        night_el = soup.select_one("[class*='Night'], [class*='night']")
        if night_el:
            info["is_night"] = 1

        return info

    def _parse_result_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """レース結果テーブルをパース

        boatrace.jpの結果ページ構造:
        tbody.is-fs12 内の各行が1着〜6着
        """
        results = []
        table = soup.select_one(".table1")
        if not table:
            return results

        for row in table.select("tbody tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue
            try:
                pos_text = cells[0].get_text(strip=True)
                if not pos_text or not pos_text[0].isdigit():
                    continue

                # 選手登番
                racer_link = row.select_one("a[href*='toban']")
                racer_id = ""
                if racer_link:
                    m = re.search(r"toban=(\d+)", racer_link.get("href", ""))
                    racer_id = m.group(1) if m else ""
                if not racer_id:
                    racer_id_text = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                    m = re.match(r"(\d{4})", racer_id_text)
                    racer_id = m.group(1) if m else ""

                result = {
                    "race_id": race_id,
                    "racer_id": racer_id,
                    "finish_position": self._safe_int(pos_text),
                    "course": self._safe_int(cells[1].get_text(strip=True)) if len(cells) > 1 else None,
                    "racer_name": cells[3].get_text(strip=True) if len(cells) > 3 else "",
                    "finish_time": cells[4].get_text(strip=True) if len(cells) > 4 else None,
                }
                if result["course"]:
                    results.append(result)
            except (IndexError, ValueError) as e:
                logger.warning("Result row parse error (race_id=%s): %s", race_id, e)
                continue

        return results

    def _parse_entry_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """出走表テーブルをパース

        boatrace.jpの出走表には:
        - 枠番 (1-6) = コース
        - 選手登番, 選手名
        - 級別 (A1/A2/B1/B2)
        - 全国勝率, 全国2連率
        - 当地勝率, 当地2連率
        - モーター番号, モーター2連率
        - ボート番号, ボート2連率
        """
        entries = []
        # boatrace.jpの出走表はtbody単位で各選手の情報がまとまっている
        for i, tbody in enumerate(soup.select(".table1 tbody"), start=1):
            if i > 6:
                break
            try:
                entry = self._parse_entry_tbody(tbody, race_id, course=i)
                if entry:
                    entries.append(entry)
            except Exception as e:
                logger.warning("Entry parse error (race_id=%s, course=%d): %s", race_id, i, e)

        return entries

    def _parse_entry_tbody(self, tbody: BeautifulSoup, race_id: str, course: int) -> dict | None:
        """1選手分のtbodyをパース"""
        rows = tbody.select("tr")
        if not rows:
            return None

        all_text = tbody.get_text(" ", strip=True)

        # 選手登番
        racer_link = tbody.select_one("a[href*='toban']")
        racer_id = ""
        if racer_link:
            m = re.search(r"toban=(\d+)", racer_link.get("href", ""))
            racer_id = m.group(1) if m else ""

        if not racer_id:
            # テキストから4桁の登番を探す
            m = re.search(r"\b(\d{4})\b", all_text)
            racer_id = m.group(1) if m else ""

        if not racer_id:
            return None

        # 選手名
        name_el = tbody.select_one(".is-fs18, .is-fs14") or racer_link
        racer_name = name_el.get_text(strip=True) if name_el else ""

        # 級別
        racer_class = None
        for cls in ["A1", "A2", "B1", "B2"]:
            if cls in all_text:
                racer_class = cls
                break

        # 数値を一括抽出（勝率, 2連率, モーター等）
        numbers = re.findall(r"(\d+\.?\d*)", all_text)
        float_numbers = [float(n) for n in numbers if "." in n]

        entry = {
            "race_id": race_id,
            "racer_id": racer_id,
            "course": course,
            "racer_name": racer_name,
            "class": racer_class,
            "branch": None,
            "national_win_rate": float_numbers[0] if len(float_numbers) > 0 else None,
            "national_2nd_rate": float_numbers[1] if len(float_numbers) > 1 else None,
            "local_win_rate": float_numbers[2] if len(float_numbers) > 2 else None,
            "local_2nd_rate": float_numbers[3] if len(float_numbers) > 3 else None,
            "motor_no": None,
            "motor_2nd_rate": float_numbers[4] if len(float_numbers) > 4 else None,
            "boat_no": None,
            "boat_2nd_rate": float_numbers[5] if len(float_numbers) > 5 else None,
            "exhibition_time": None,
            "start_timing": None,
        }

        # 支部 (県名)
        branch_el = tbody.select_one(".is-fs11")
        if branch_el:
            entry["branch"] = branch_el.get_text(strip=True)

        # モーター/ボート番号
        int_numbers = [int(n) for n in numbers if "." not in n and len(n) <= 3]
        # 登番(4桁)以外の2-3桁の数値からモーター・ボート番号を推定
        small_ints = [n for n in int_numbers if 1 <= n <= 999 and n != course]
        if len(small_ints) >= 2:
            entry["motor_no"] = small_ints[0]
            entry["boat_no"] = small_ints[1]
        elif len(small_ints) == 1:
            entry["motor_no"] = small_ints[0]

        return entry

    def _merge_before_info(self, soup: BeautifulSoup, entries: list[dict]):
        """直前情報ページから展示タイム・STを各エントリに追加"""
        # 展示タイムテーブル
        for i, tbody in enumerate(soup.select(".table1 tbody")):
            if i >= len(entries):
                break
            text = tbody.get_text(" ", strip=True)
            # 展示タイムは "6.xx" のような形式
            times = re.findall(r"\b(\d\.\d{2})\b", text)
            if times:
                entries[i]["exhibition_time"] = float(times[0])
            # 平均STは "-0.xx" or "0.xx" のような形式
            st_match = re.search(r"(-?\d+\.\d{2})", text)
            if st_match and entries[i]["exhibition_time"] != float(st_match.group(1)):
                entries[i]["start_timing"] = float(st_match.group(1))

    def _parse_weather(self, soup: BeautifulSoup) -> dict:
        """天候・風・波高をパース"""
        info = {}
        weather_area = soup.select_one(".weather1")
        if not weather_area:
            return info

        text = weather_area.get_text(" ", strip=True)

        # 天候
        for w in ["晴", "曇り", "雨", "雪", "霧"]:
            if w in text:
                info["weather"] = w
                break

        # 風向
        for d in ["北", "北東", "東", "南東", "南", "南西", "西", "北西", "無風"]:
            if d in text:
                info["wind_direction"] = d
                break

        # 風速
        wind_match = re.search(r"(\d+)m", text)
        if wind_match:
            info["wind_speed"] = float(wind_match.group(1))

        # 波高
        wave_match = re.search(r"(\d+)cm", text)
        if wave_match:
            info["wave_height"] = float(wave_match.group(1))

        return info

    # ── ユーティリティ ──

    @staticmethod
    def _safe_int(text: str) -> int | None:
        try:
            return int(re.sub(r"[^\d]", "", text))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_float(text: str) -> float | None:
        try:
            return float(text.replace(",", ""))
        except (ValueError, TypeError):
            return None
