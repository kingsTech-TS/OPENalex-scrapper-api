from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import requests
import pandas as pd
import io
import time
import random

app = FastAPI(title="OpenAlex Book Scraper API")

# ✅ Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specify your frontend origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OPENALEX_BASE = "https://api.openalex.org"


@app.get("/")
def root():
    return {
        "message": "📚 Welcome to the OpenAlex Book Scraper API",
        "endpoints": {
            "books": "/books?subjects=Marketing&start_year=2021&end_year=2025&max_results=50&format=json"
        },
        "note": "Visit /docs for interactive API documentation"
    }


def pick_best_url(work: dict) -> str:
    pl = work.get("primary_location") or {}
    if pl.get("pdf_url"):  # Prefer direct PDF link
        return pl["pdf_url"]
    if pl.get("landing_page_url"):
        return pl["landing_page_url"]
    doi = (work.get("ids") or {}).get("doi")
    if doi:
        return doi
    return work.get("id", "")


def resolve_subject_id(subject: str, session: requests.Session, mailto: str = None):
    """Resolve subject to an OpenAlex concept or topic ID (prefer concepts)."""
    params = {"search": subject, "per-page": 1}
    if mailto:
        params["mailto"] = mailto

    # ✅ Try concepts first (broader)
    r = session.get(f"{OPENALEX_BASE}/concepts", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("results"):
        return "concepts.id", data["results"][0]["id"]

    # Then fallback to topics (narrower)
    r = session.get(f"{OPENALEX_BASE}/topics", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("results"):
        return "topics.id", data["results"][0]["id"]

    return None, None


def request_with_backoff(session, url, params, max_retries=5):
    """Handles 429 errors with exponential backoff."""
    for attempt in range(max_retries):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 429:
            sleep_time = 2 ** attempt + random.random()
            time.sleep(sleep_time)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()


def search_books_by_subject(subject, start_year=2021, end_year=2025, max_results=50, mailto=None, oa_only=True):
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    key, id_url = resolve_subject_id(subject, session, mailto)
    if not key:
        return []

    # Build filter
    filter_parts = [f"type:book", f"{key}:{id_url}", f"publication_year:{start_year}-{end_year}"]
    if oa_only:
        filter_parts.append("is_oa:true")

    params = {
        "filter": ",".join(filter_parts),
        "per-page": 50,
    }
    if mailto:
        params["mailto"] = mailto

    all_rows, page = [], 1
    while len(all_rows) < max_results:
        params["page"] = page
        print("🔍 Querying:", f"{OPENALEX_BASE}/works", params)  # ✅ Debug query
        resp = request_with_backoff(session, f"{OPENALEX_BASE}/works", params=params)
        results = resp.json().get("results", [])
        if not results:
            break

        for work in results:
            # ✅ Only English
            if work.get("language") != "en":
                continue

            # ✅ Require free access URL
            url = pick_best_url(work)
            if not url:
                continue

            title = work.get("display_name") or "N/A"
            year = work.get("publication_year")
            year = int(year) if isinstance(year, int) else 0
            authors = [
                (a.get("author") or {}).get("display_name")
                for a in work.get("authorships", [])
                if (a.get("author") or {}).get("display_name")
            ]
            all_rows.append(
                {
                    "Title": title,
                    "Authors": ", ".join(authors),
                    "Year": year,
                    "URL": url,
                    "Subject": subject,
                }
            )
            if len(all_rows) >= max_results:
                break
        page += 1

    return all_rows


@app.get("/books")
def get_books(
    subjects: str = Query(..., description="Comma-separated subjects, e.g., Marketing,Chemistry"),
    start_year: int = 2021,
    end_year: int = 2025,
    max_results: int = 50,
    mailto: str = None,
    oa_only: bool = Query(True, description="Require Open Access (true/false)"),
    format: str = Query("json", description="Output format: json or csv"),
):
    subject_list = [s.strip() for s in subjects.split(",") if s.strip()]
    results = []

    for subject in subject_list:
        rows = search_books_by_subject(subject, start_year, end_year, max_results, mailto, oa_only)
        results.extend(rows)

        # ✅ Fallback: if too few results with OA, retry without OA
        if oa_only and len(rows) < max_results // 5:
            print(f"⚠️ Few results for {subject} with OA filter — retrying without OA")
            rows = search_books_by_subject(subject, start_year, end_year, max_results, mailto, oa_only=False)
            results.extend(rows)

    if not results:
        return JSONResponse(
            content={"message": "No results found for given subjects."},
            status_code=404
        )

    # ✅ Sort by Year descending
    results.sort(key=lambda x: x["Year"], reverse=True)

    if format == "csv":
        df = pd.DataFrame(results)
        buf = io.StringIO()
        df.to_csv(buf, index=False, encoding="utf-8")
        buf.seek(0)
        filename = f"books_{'_'.join(subject_list)}.csv"
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    return JSONResponse(content=results)
