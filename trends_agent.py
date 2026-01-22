from pytrends.exceptions import TooManyRequestsError
import os, json, time, requests
from datetime import datetime, timezone

from pytrends.request import TrendReq
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

def utc_date():
  return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def pick_title(t):
  return (t.get("english") or t.get("romaji") or t.get("native") or "").strip()

def fetch_anilist(limit=30):
  per_page = min(limit, 50)
  r = requests.post(
    ANILIST_URL,
    json={"query": ANILIST_QUERY, "variables": {"page": 1, "perPage": per_page}},
    timeout=30,
  )
  r.raise_for_status()
  items = r.json()["data"]["Page"]["media"][:limit]

  out = []
  for m in items:
    title = pick_title(m["title"])
    trending = int(m.get("trending") or 0)
    popularity = int(m.get("popularity") or 0)
    fav = int(m.get("favourites") or 0)
    score = trending * 2 + popularity * 0.01 + fav * 0.05
    out.append({"title": title, "source": "AniList", "score": round(score, 2),
                "details": f"trending={trending}, pop={popularity}, fav={fav}"})
  return out

def fetch_google_trends(country="MA", limit=20):
  pytrends = TrendReq(hl="en-US", tz=0)
  seeds = ["manga", "manhwa", "webtoon", "anime manga"]
  rows = []

  for seed in seeds:
    pytrends.build_payload([seed], geo=country, timeframe="now 7-d")
    related = pytrends.related_queries().get(seed, {})
    rising = related.get("rising")
    if rising is None:
      continue
    for _, r in rising.head(limit).iterrows():
      q = str(r["query"]).strip()
      v = float(r["value"])
      rows.append((q, v, seed))
    time.sleep(1)

  best = {}
  for q, v, seed in rows:
    if q not in best or v > best[q]["value"]:
      best[q] = {"value": v, "seed": seed}

  out = []
  for q, meta in sorted(best.items(), key=lambda x: x[1]["value"], reverse=True)[:limit]:
    out.append({"title": q, "source": "GoogleTrends", "score": round(float(meta["value"]), 2),
                "details": f"seed={meta['seed']}, rising={meta['value']}"})
  return out

def open_sheet():
  sheet_id = os.environ["SHEET_ID"]
  sa_json = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
  scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
  creds = Credentials.from_service_account_info(sa_json, scopes=scopes)
  gc = gspread.authorize(creds)
  return gc.open_by_key(sheet_id)

def read_config(sh):
  cfg = {"country": "MA", "limit": 30}
  try:
    ws = sh.worksheet("Config")
    rows = ws.get_all_values()[1:]
    for k, v in rows:
      k = (k or "").strip()
      v = (v or "").strip()
      if k == "country" and v: cfg["country"] = v
      if k == "limit" and v.isdigit(): cfg["limit"] = int(v)
  except Exception:
    pass
  return cfg

def main():
  sh = open_sheet()
  cfg = read_config(sh)
  date = utc_date()

  anilist = fetch_anilist(limit=cfg["limit"])
  gtrends = fetch_google_trends(country=cfg["country"], limit=min(20, cfg["limit"]))

  merged = []
  for it in anilist + gtrends:
    merged.append([date, it["title"], it["source"], it["score"], it["details"]])

  merged.sort(key=lambda r: float(r[3]), reverse=True)

  ws = sh.worksheet("DailyTrends")
  ws.append_rows(merged, value_input_option="USER_ENTERED")
  print(f"Wrote {len(merged)} rows for {date}")

if __name__ == "__main__":
  main()
