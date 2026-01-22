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
      status
    }
  }
}
"""


def utc_date_str():
  return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def pick_title(t):
  return (t.get("english") or t.get("romaji") or t.get("native") or "").strip()


def normalize_status(anilist_status):
  if anilist_status == "RELEASING":
    return "Ongoing"
  if anilist_status == "FINISHED":
    return "Completed"
  if anilist_status == "HIATUS":
    return "Hiatus"
  return "Unknown"


def fetch_anilist_trending(limit=30):
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
    status = normalize_status(m.get("status"))

    score = trending * 2 + popularity * 0.01 + favourites * 0.05

    results.append({
      "title": title,
      "source": "AniList",
      "score": round(score, 2),
      "details": f"trending={trending}, pop={popularity}, fav={favourites}",
      "status": status,
    })

  return results


def fetch_google_trends(country="MA", limit=20):
  try:
    pytrends = TrendReq(hl="en-US", tz=0, retries=3, backoff_factor=2)
  except TypeError:
    pytrends = TrendReq(hl="en-US", tz=0)

  seeds = ["manga", "manhwa", "webtoon", "anime manga"]
  all_rows = []

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

      time.sleep(3)

    except TooManyRequestsError:
      print("⚠️ Google Trends rate-limited (429). Skipping Google Trends.")
      return []
    except Exception:
      return []

  best = {}
  for q, v, seed in all_rows:
    if q not in best or v > best[q]["value"]:
      best[q] = {"value": v, "seed": seed}

  results = []
  for q, meta in sorted(best.items(), key=lambda x: x[1]["value"], reverse=True)[:limit]:
    results.append({
      "title": q,
      "source": "GoogleTrends",
      "score": round(float(meta["value"]), 2),
      "details": f"seed={meta['seed']}, rising={meta['value']}",
      "status": "Unknown",
    })

  return results


def get_gspread_client():
  info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
  creds = Credentials.from_service_account_info(
    info,
    scopes=[
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/drive",
    ],
  )
  return gspread.authorize(creds).open_by_key(os.environ["SHEET_ID"])


def read_config(sh):
  cfg = {"country": "MA", "limit": 30}
  try:
    ws = sh.worksheet("Config")
    for k, v in ws.get_all_values()[1:]:
      if k == "country" and v:
        cfg["country"] = v
      if k == "limit" and v.isdigit():
        cfg["limit"] = int(v)
  except Exception:
    pass
  return cfg


def main():
  sh = get_gspread_client()
  cfg = read_config(sh)

  date = utc_date_str()
  limit = cfg["limit"]
  country = cfg["country"]

  anilist = fetch_anilist_trending(limit)
  gtrends = fetch_google_trends(country, min(20, limit))

  rows = []
  for item in anilist + gtrends:
    rows.append([
      date,
      item["title"],
      item["source"],
      item["score"],
      item["details"],
      item["status"],
    ])

  rows.sort(key=lambda r: float(r[3]), reverse=True)

  sh.worksheet("DailyTrends").append_rows(rows, value_input_option="USER_ENTERED")
  print(f"✅ Wrote {len(rows)} rows for {date}")


if __name__ == "__main__":
  main()
