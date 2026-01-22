import os
import json
import requests
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials


ANILIST_URL = "https://graphql.anilist.co"

ANILIST_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    media(sort: TRENDING_DESC, type: MANGA, status: FINISHED) {
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


def fetch_completed_anilist(limit=30):
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

    results.append({
      "title": title,
      "source": "AniList",
      "score": round(score, 2),
      "details": f"trending={trending}, pop={popularity}, fav={favourites}",
      "status": "Completed",
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
  cfg = {"limit": 30}
  try:
    ws = sh.worksheet("Config")
    for k, v in ws.get_all_values()[1:]:
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

  completed = fetch_completed_anilist(limit)

  rows = []
  for item in completed:
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
  print(f"✅ Wrote {len(rows)} COMPLETED manga for {date}")


if __name__ == "__main__":
  main()
