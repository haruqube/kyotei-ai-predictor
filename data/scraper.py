"""boatrace.jpスクレイパー（キャッシュ付き）

実際のHTML構造に基づく実装 (2026-03確認済み):

出走表 (racelist):
  div.table1.is-tableFixed__3rdadd 内の tbody.is-fs12 × 6 (各選手)
  各tbodyの1行目:
    td[0]: 枠番 (is-boatColor{N})
    td[1]: 写真 (a[href*=toban])
    td[2]: 登番+級別+名前+支部 (div.is-fs11, span, div.is-fs18, div.is-fs11)
    td[3]: F/L/平均ST (lineH2)
    td[4]: 全国勝率/2連率/3連率 (lineH2)
    td[5]: 当地勝率/2連率/3連率 (lineH2)
    td[6]: モーター番号/2連率/3連率 (lineH2)
    td[7]: ボート番号/2連率/3連率 (lineH2)

結果 (raceresult):
  div.table1 > table.is-w495 内の tbody × 6 (各着順)
  各tbody > tr:
    td[0]: 着順 (全角数字)
    td[1]: 枠番 (is-boatColor{N})
    td[2]: span.is-fs12=登番 + span.is-fs18=選手名
    td[3]: レースタイム

直前情報 (beforeinfo):
  tbody.is-fs12 × 6:
    td[4] (rowspan=4): 展示タイム (6.83等)

天候 (weather1):
  div.weather1_bodyUnit.is-weather → 天候
  div.weather1_bodyUnit.is-wind → 風速 (Nm)
  div.weather1_bodyUnit.is-wave → 波高 (Ncm)
  div.weather1_bodyUnit.is-windDirection → p.is-wind{NN} (風向の角度コード)
"""

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

# 風向画像コード → 風向名マッピング (is-wind{NN})
_WIND_DIR_MAP = {
    "1": "北", "2": "北北東", "3": "北東", "4": "東北東",
    "5": "東", "6": "東南東", "7": "南東", "8": "南南東",
    "9": "南", "10": "南南西", "11": "南西", "12": "西南西",
    "13": "西", "14": "西北西", "15": "北西", "16": "北北西",
    "17": "無風",
}


class BoatraceScraper:
    """boatrace.jpからレース情報・結果をスクレイピング"""

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

        レース一覧ページから a[href*="raceindex?jcd="] で開催場コードを抽出し、
        各場12レースのrace_idを生成。
        """
        cache_key = f"race_list_{date}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/index?hd={date}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        venue_codes = []
        for link in soup.select("a[href*='raceindex?jcd=']"):
            href = link.get("href", "")
            m = re.search(r"jcd=(\d{2})", href)
            if m and m.group(1) not in venue_codes:
                venue_codes.append(m.group(1))

        race_ids = []
        for vc in sorted(venue_codes):
            for rno in range(1, 13):
                race_ids.append(f"{date}_{vc}_{rno:02d}")

        self._set_json_cache(cache_key, race_ids)
        return race_ids

    def _parse_race_id(self, race_id: str) -> tuple[str, str, int]:
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
        weather = self._parse_weather(soup)
        race_info.update(weather)
        race_info["results"] = results
        race_info["racer_count"] = len(results)

        self._set_json_cache(cache_key, race_info)
        return race_info

    def scrape_race_entry(self, race_id: str) -> dict:
        """出走表 + 直前情報をパース"""
        cache_key = f"race_entry_{race_id}"
        cached = self._get_json_cache(cache_key)
        if cached:
            return cached

        date, jcd, rno = self._parse_race_id(race_id)

        # 出走表
        url = f"{BOATRACE_BASE_URL}/owpc/pc/race/racelist?rno={rno}&jcd={jcd}&hd={date}"
        html = self._get(url)
        soup = BeautifulSoup(html, "lxml")

        race_info = self._parse_race_info(soup, race_id, date, jcd, rno)
        entries = self._parse_entry_table(soup, race_id)
        race_info["entries"] = entries
        race_info["racer_count"] = len(entries)

        # 直前情報（展示タイム等）
        before_url = f"{BOATRACE_BASE_URL}/owpc/pc/race/beforeinfo?rno={rno}&jcd={jcd}&hd={date}"
        before_html = self._get(before_url)
        before_soup = BeautifulSoup(before_html, "lxml")
        self._merge_before_info(before_soup, entries)

        weather = self._parse_weather(before_soup)
        race_info.update(weather)

        self._set_json_cache(cache_key, race_info)
        return race_info

    # ── レース情報 ──

    def _parse_race_info(self, soup: BeautifulSoup, race_id: str,
                         date: str, jcd: str, rno: int) -> dict:
        info = {
            "race_id": race_id,
            "venue_code": jcd,
            "venue_name": VENUE_CODES.get(jcd, jcd),
            "race_number": rno,
        }

        # レース名: h2.heading2_titleName
        title = soup.select_one(".heading2_titleName")
        info["race_name"] = title.get_text(strip=True) if title else ""

        # グレード: heading2_titleName周辺のclass or テキストから判定
        info["grade"] = None
        page_text = soup.get_text()
        for grade_cls, grade_name in [("is-SGrade", "SG"), ("is-G1", "G1"),
                                       ("is-G2", "G2"), ("is-G3", "G3")]:
            if soup.select_one(f"[class*='{grade_cls}']"):
                info["grade"] = grade_name
                break

        # ナイター判定
        info["is_night"] = 0
        if soup.select_one("[class*='is-night'], [class*='Night']"):
            info["is_night"] = 1

        return info

    # ── 結果テーブル ──

    def _parse_result_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """結果テーブルをパース

        構造: div.table1 > table.is-w495 内
        各着順は独立したtbody。tbody > tr > td が4列:
          [0] 着順(全角), [1] 枠番(is-boatColor), [2] 登番+名前, [3] タイム
        """
        results = []

        # table.is-w495 が結果テーブル
        result_table = soup.select_one("table.is-w495")
        if not result_table:
            return results

        for tbody in result_table.select("tbody"):
            row = tbody.select_one("tr")
            if not row:
                continue
            cells = row.select("td")
            if len(cells) < 4:
                continue

            try:
                # 着順: 全角数字 → int
                pos_text = cells[0].get_text(strip=True)
                pos = self._zenkaku_to_int(pos_text)
                if pos is None:
                    continue

                # 枠番(コース): is-boatColor{N}のテキスト
                course = self._safe_int(cells[1].get_text(strip=True))

                # 登番 + 選手名: span.is-fs12 = 登番, span.is-fs18 = 名前
                racer_id = ""
                racer_name = ""
                toban_span = cells[2].select_one("span.is-fs12")
                name_span = cells[2].select_one("span.is-fs18, span.is-fBold")
                if toban_span:
                    racer_id = toban_span.get_text(strip=True)
                if name_span:
                    racer_name = name_span.get_text(strip=True)

                # タイム
                finish_time = cells[3].get_text(strip=True) if len(cells) > 3 else None

                if course and racer_id:
                    results.append({
                        "race_id": race_id,
                        "racer_id": racer_id,
                        "finish_position": pos,
                        "course": course,
                        "racer_name": racer_name,
                        "finish_time": finish_time,
                    })
            except (IndexError, ValueError) as e:
                logger.warning("Result parse error (race_id=%s): %s", race_id, e)

        return results

    # ── 出走表テーブル ──

    def _parse_entry_table(self, soup: BeautifulSoup, race_id: str) -> list[dict]:
        """出走表をパース

        構造: div.table1.is-tableFixed__3rdadd 内の tbody.is-fs12 × 6
        各tbodyの最初のtr:
          td[0]: 枠番 (is-boatColor{N}, rowspan=4)
          td[1]: 写真 (rowspan=4, a[href*=toban])
          td[2]: 登番+級別+名前+支部 (rowspan=4)
          td[3]: F回数/L回数/平均ST (rowspan=4)
          td[4]: 全国勝率/2連率/3連率 (rowspan=4)
          td[5]: 当地勝率/2連率/3連率 (rowspan=4)
          td[6]: モーター番号/2連率/3連率 (rowspan=4)
          td[7]: ボート番号/2連率/3連率 (rowspan=4)
        """
        entries = []

        # is-tableFixed__3rdadd テーブルを探す
        entry_table = soup.select_one(".is-tableFixed__3rdadd")
        if not entry_table:
            return entries

        for tbody in entry_table.select("tbody.is-fs12"):
            try:
                entry = self._parse_entry_tbody(tbody, race_id)
                if entry:
                    entries.append(entry)
            except Exception as e:
                logger.warning("Entry parse error (race_id=%s): %s", race_id, e)

        return entries

    def _parse_entry_tbody(self, tbody, race_id: str) -> dict | None:
        """1選手分のtbodyをパース"""
        first_row = tbody.select_one("tr")
        if not first_row:
            return None

        cells = first_row.select("td")
        if len(cells) < 7:
            return None

        # td[0]: 枠番
        course = self._safe_int(cells[0].get_text(strip=True))
        if not course:
            return None

        # td[1]: 写真 → tobanリンクから登番取得
        racer_id = ""
        toban_link = tbody.select_one("a[href*='toban=']")
        if toban_link:
            m = re.search(r"toban=(\d+)", toban_link.get("href", ""))
            racer_id = m.group(1) if m else ""
        if not racer_id:
            return None

        # td[2]: 登番/級別/名前/支部
        info_cell = cells[2]

        # 級別: span内テキスト
        racer_class = None
        class_span = info_cell.select_one("div.is-fs11 span")
        if class_span:
            cls_text = class_span.get_text(strip=True)
            if cls_text in ("A1", "A2", "B1", "B2"):
                racer_class = cls_text

        # 名前: div.is-fs18
        name_el = info_cell.select_one("div.is-fs18")
        racer_name = name_el.get_text(strip=True) if name_el else ""

        # 支部: div.is-fs11 の2番目 (県名/支部)
        branch = None
        fs11_divs = info_cell.select("div.is-fs11")
        if len(fs11_divs) >= 2:
            branch_text = fs11_divs[1].get_text(strip=True)
            # "埼玉/埼玉" → "埼玉"
            parts = branch_text.split("/")
            branch = parts[0].strip() if parts else branch_text

        # td[3]: F回数/L回数/平均ST
        st_cell_text = cells[3].get_text(" ", strip=True)
        avg_st = None
        st_numbers = re.findall(r"(\d+\.\d+)", st_cell_text)
        if st_numbers:
            avg_st = float(st_numbers[-1])  # 最後の数値が平均ST

        # td[4]: 全国勝率/2連率/3連率
        national_vals = self._parse_rate_cell(cells[4])

        # td[5]: 当地勝率/2連率/3連率
        local_vals = self._parse_rate_cell(cells[5])

        # td[6]: モーター番号/2連率/3連率
        motor_vals = self._parse_equipment_cell(cells[6])

        # td[7]: ボート番号/2連率/3連率
        boat_vals = self._parse_equipment_cell(cells[7])

        return {
            "race_id": race_id,
            "racer_id": racer_id,
            "course": course,
            "racer_name": racer_name,
            "class": racer_class,
            "branch": branch,
            "national_win_rate": national_vals[0],
            "national_2nd_rate": national_vals[1],
            "local_win_rate": local_vals[0],
            "local_2nd_rate": local_vals[1],
            "motor_no": motor_vals[0],
            "motor_2nd_rate": motor_vals[1],
            "boat_no": boat_vals[0],
            "boat_2nd_rate": boat_vals[1],
            "exhibition_time": None,
            "start_timing": avg_st,
        }

    def _parse_rate_cell(self, cell) -> tuple:
        """勝率/2連率/3連率のセル → (勝率, 2連率)"""
        text = cell.get_text(" ", strip=True)
        numbers = re.findall(r"(\d+\.\d+)", text)
        win_rate = float(numbers[0]) if len(numbers) > 0 else None
        rate_2nd = float(numbers[1]) if len(numbers) > 1 else None
        return (win_rate, rate_2nd)

    def _parse_equipment_cell(self, cell) -> tuple:
        """モーター/ボートのセル → (番号, 2連率)"""
        text = cell.get_text(" ", strip=True)
        numbers = re.findall(r"(\d+\.?\d*)", text)
        equip_no = None
        rate_2nd = None
        for n in numbers:
            if "." in n:
                if rate_2nd is None:
                    rate_2nd = float(n)
            else:
                if equip_no is None:
                    equip_no = int(n)
        return (equip_no, rate_2nd)

    # ── 直前情報 ──

    def _merge_before_info(self, soup: BeautifulSoup, entries: list[dict]):
        """直前情報ページから展示タイムを各エントリに追加

        構造: tbody.is-fs12 × 6、各tbody内で td[4](rowspan=4) が展示タイム
        """
        tbodies = soup.select("tbody.is-fs12")
        for i, tbody in enumerate(tbodies):
            if i >= len(entries):
                break

            first_row = tbody.select_one("tr")
            if not first_row:
                continue

            cells = first_row.select("td")
            # 展示タイムは5番目のtd (index=4): "6.83"のような値
            if len(cells) >= 5:
                et_text = cells[4].get_text(strip=True)
                et = self._safe_float(et_text)
                if et and 5.0 < et < 10.0:
                    entries[i]["exhibition_time"] = et

    # ── 天候 ──

    def _parse_weather(self, soup: BeautifulSoup) -> dict:
        """天候・風・波高をパース

        構造: div.weather1 内の weather1_bodyUnit:
          .is-weather → 天候 (weather1_bodyUnitLabelTitle)
          .is-wind → 風速 (weather1_bodyUnitLabelData: "Nm")
          .is-windDirection → p.is-wind{NN} (風向コード)
          .is-wave → 波高 (weather1_bodyUnitLabelData: "Ncm")
        """
        info = {}
        weather_area = soup.select_one(".weather1")
        if not weather_area:
            return info

        # 天候
        weather_unit = weather_area.select_one(".weather1_bodyUnit.is-weather")
        if weather_unit:
            title = weather_unit.select_one(".weather1_bodyUnitLabelTitle")
            if title:
                info["weather"] = title.get_text(strip=True)

        # 風速
        wind_unit = weather_area.select_one(".weather1_bodyUnit.is-wind")
        if wind_unit:
            data = wind_unit.select_one(".weather1_bodyUnitLabelData")
            if data:
                m = re.search(r"(\d+)m", data.get_text(strip=True))
                if m:
                    info["wind_speed"] = float(m.group(1))

        # 風向: p要素のclass is-wind{NN} から判定
        wind_dir_unit = weather_area.select_one(".weather1_bodyUnit.is-windDirection")
        if wind_dir_unit:
            p_el = wind_dir_unit.select_one("p[class*='is-wind']")
            if p_el:
                for cls in p_el.get("class", []):
                    m = re.search(r"is-wind(\d+)", cls)
                    if m:
                        info["wind_direction"] = _WIND_DIR_MAP.get(m.group(1), "不明")
                        break

        # 波高
        wave_unit = weather_area.select_one(".weather1_bodyUnit.is-wave")
        if wave_unit:
            data = wave_unit.select_one(".weather1_bodyUnitLabelData")
            if data:
                m = re.search(r"(\d+)cm", data.get_text(strip=True))
                if m:
                    info["wave_height"] = float(m.group(1))

        return info

    # ── ユーティリティ ──

    @staticmethod
    def _zenkaku_to_int(text: str) -> int | None:
        """全角数字 → int ('１' → 1)"""
        zen = "０１２３４５６７８９"
        result = ""
        for c in text.strip():
            if c in zen:
                result += str(zen.index(c))
            elif c.isdigit():
                result += c
        return int(result) if result else None

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
