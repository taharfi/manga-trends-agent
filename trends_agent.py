import os
import json
import time
import requests
from datetime import datetime, timezone

from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

import gspread
from google.oauth2.service_account import Credentials


ANILIST_URL = "https://graphql.anilist.co"

ANILIST_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(sort: TRENDING_DESC, type: MANGA) {
      title { romaji english native }
      trending
      popularity
      favourites
    }
  }
}
"""


def utc_date_str() -> str:
  return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def pick_title(t: dict) -> str:
  return (t.get("english") or t.get("romaji") or t.get("native") or "").strip()


def fetch_anilist_trending(limit: int = 30) -> list[dict]:
  per_page = min(limit, 50)
  r = requests.post(
    ANILIST_URL,
    json={"query": ANILIST_QUERY, "variables": {"page": 1, "perPage": per_page}},
    timeout=30,
  )
  r.raise_for_status()
  items = r.json()["data"]["Page"]["media"][:limit]

  results = []
  for m in items:
    title = pick_title(m["title"])
    trending = int(m.get("trending") or 0)
    popularity = int(m.get("popularity") or 0)
    favourites = int(m.get("favourites") or 0)

    score = trending * 2 + popularity * 0.01 + favourites * 0.05

    results.append(
      {
        "title": title,
        "source": "AniList",
        "score": round(score, 2),
        "details": f"trending={trending}, pop={popularity}, fav={favourites}",
      }
    )
  return results


def fetch_google_trends(country: str = "MA", limit: int = 20) -> list[dict]:
  """
  Google Trends often rate-limits cloud IPs (429), especially on GitHub Actions.
  This function will NEVER crash the workflow:
  - On 429: returns []
  - On any other error: returns []
  """
  try:
    # some pytrends versions support retries/backoff
    pytrends = TrendReq(hl="en-US", tz=0, retries=3, backoff_factor=2)
  except TypeError:
    pytrends = TrendReq(hl="en-US", tz=0)

  seeds = ["manga", "manhwa", "webtoon", "anime manga"]
  all_rows: list[tuple[str, float, str]] = []

  for seed in seeds:
    try:
      pytrends.build_payload([seed], geo=country, timeframe="now 7-d")
      related = pytrends.related_queries()
      rq = related.get(seed, {}).get("rising")

      if rq is None:
        continue

      for _, row in rq.head(limit).iterrows():
        q = str(row["query"]).strip()
        v = float(row["value"])
        all_rows.append((q, v, seed))

      # stronger delay to reduce rate-limits
      time.sleep(3)

    except TooManyRequestsError:
      print("⚠️ Google Trends rate-limited (429). Skipping Google Trends for this run.")
      return []
    except Exception as e:
      print(f"⚠️ Google Trends error for seed '{seed}': {e}. Skipping Google Trends.")
      return []

  # Deduplicate by query, keep max value
  best: dict[str, dict] = {}
  for q, v, seed in all_rows:
    if q not in best or v > best[q]["value"]:
      best[q] = {"value": v, "seed": seed}

  results: list[dict] = []
  for q, meta in sorted(best.items(), key=lambda x: x[1]["value"], reverse=True)[:limit]:
    results.append(
      {
        "title": q,
        "source": "GoogleTrends",
        "score": round(float(meta["value"]), 2),
        "details": f"seed={meta['seed']}, rising={meta['value']}",
      }
    )
  return results


def get_gspread_client():
  sheet_id = os.environ["SHEET_ID"]
  sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

  info = json.loads(sa_json)
  scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
  ]
  creds = Credentials.from_service_account_info(info, scopes=scopes)
  gc = gspread.authorize(creds)
  sh = gc.open_by_key(sheet_id)
  return sh


def read_config(sh):
  cfg = {"country": "MA", "limit": 30}
  try:
    ws = sh.worksheet("Config")
    rows = ws.get_all_values()[1:]  # skip headers
    for k, v in rows:
      k = (k or "").strip()
      v = (v or "").strip()
      if k == "country" and v:
        cfg["country"] = v
      if k == "limit" and v.isdigit():
        cfg["limit"] = int(v)
  except Exception:
    # if Config tab doesn't exist, keep defaults
    pass
  return cfg


def append_rows(sh, rows: list[list]):
  ws = sh.worksheet("DailyTrends")
  ws.append_rows(rows, value_input_option="USER_ENTERED")


def main():
  sh = get_gspread_client()
  cfg = read_config(sh)

  date = utc_date_str()
  limit = int(cfg["limit"])
  country = cfg["country"]

  anilist = fetch_anilist_trending(limit=limit)
  gtrends = fetch_google_trends(country=country, limit=min(20, limit))

  merged: list[list] = []
  for item in anilist:
    merged.append([date, item["title"], item["source"], item["score"], item["details"]])
  for item in gtrends:
    merged.append([date, item["title"], item["source"], item["score"], item["details"]])

  merged.sort(key=lambda r: float(r[3]), reverse=True)

  append_rows(sh, merged)
  print(f"✅ Wrote {len(merged)} rows for {date} (country={country}, limit={limit})")


if __name__ == "__main__":
  main()
