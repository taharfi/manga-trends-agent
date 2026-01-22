import os
import json
import requests
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials


ANILIST_URL = "https://graphql.anilist.co"

# 1) Global trending manga (any status)
ANILIST_TRENDING_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(sort: TRENDING_DESC, type: MANGA) {
      id
      title { romaji english native }
      trending
      popularity
      favourites
      status
    }
  }
}
"""

# 2) Fallback: Popular finished manga (guaranteed to have results)
ANILIST_FINISHED_POPULAR_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(sort: POPULARITY_DESC, type: MANGA, status: FINISHED) {
      id
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


def normalize_status(s):
  if s == "FINISHED":
    return "Completed"
  if s == "RELEASING":
    return "Ongoing"
  if s == "HIATUS":
    return "Hiatus"
  return "Unknown"


def score_item(trending, popularity, favourites):
  trending = int(trending or 0)
  popularity = int(popularity or 0)
  favourites = int(favourites or 0)
  return round(trending * 2 + popularity * 0.01 + favourites * 0.05, 2)


def anilist_fetch(query, page=1, per_page=50):
  r = requests.post(
    ANILIST_URL,
    json={"query": query, "variables": {"page": page, "perPage": per_page}},
    timeout=30,
  )
  r.raise_for_status()
  return r.json()["data"]["Page"]["media"]


def fetch_completed_global(limit=30):
  """
  Strategy:
  - Pull global TRENDING manga (any status), filter locally for FINISHED.
  - If too few, fill remaining with POPULARITY_FINISHED fallback.
  """
  per_page = 50  # max per page for our use
  trending = anilist_fetch(ANILIST_TRENDING_QUERY, page=1, per_page=per_page)

  completed = []
  seen_ids = set()

  # Take FINISHED from trending
  for m in trending:
    if m.get("status") == "FINISHED":
      mid = m.get("id")
      if mid in seen_ids:
        continue
      seen_ids.add(mid)

      title = pick_title(m["title"])
      sc = score_item(m.get("trending"), m.get("popularity"), m.get("favourites"))

      completed.append({
        "title": title,
        "source": "AniList(Trending)",
        "score": sc,
        "details": f"trending={m.get('trending')}, pop={m.get('popularity')}, fav={m.get('favourites')}",
        "status": "Completed",
      })
      if len(completed) >= limit:
        return completed

  # Fallback if not enough completed titles appear in trending
  if len(completed) < limit:
    need = limit - len(completed)
    popular_finished = anilist_fetch(ANILIST_FINISHED_POPULAR_QUERY, page=1, per_page=per_page)

    for m in popular_finished:
      mid = m.get("id")
      if mid in seen_ids:
        continue
      seen_ids.add(mid)

      title = pick_title(m["title"])
      sc = score_item(m.get("trending"), m.get("popularity"), m.get("favourites"))

      completed.append({
        "title": title,
        "source": "AniList(PopularFinished)",
        "score": sc,
        "details": f"trending={m.get('trending')}, pop={m.get('popularity')}, fav={m.get('favourites')}",
        "status": "Completed",
      })
      if len(completed) >= limit:
        break

  return completed


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
  cfg = {"limit": 30}
  try:
    ws = sh.worksheet("Config")
    for k, v in ws.get_all_values()[1:]:
      k = (k or "").strip()
      v = (v or "").strip()
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

  completed = fetch_completed_global(limit=limit)

  rows = []
  for it in completed:
    rows.append([date, it["title"], it["source"], it["score"], it["details"], it["status"]])

  # sort by score descending
  rows.sort(key=lambda r: float(r[3]), reverse=True)

  sh.worksheet("DailyTrends").append_rows(rows, value_input_option="USER_ENTERED")
  print(f"✅ Wrote {len(rows)} COMPLETED titles for {date}")


if __name__ == "__main__":
  main()
