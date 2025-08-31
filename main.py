from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse
import requests
import pandas as pd
import io
import time

app = FastAPI(title="OpenAlex Book Scraper API")

OPENALEX_BASE = "https://api.openalex.org"


def pick_best_url(work: dict) -> str:
    pl = work.get("primary_location") or {}
    if pl.get("landing_page_url"):
        return pl["landing_page_url"]
    if pl.get("pdf_url"):
        return pl["pdf_url"]
    doi = (work.get("ids") or {}).get("doi")
    if doi:
        return doi
    return work.get("id", "")


def resolve_subject_id(subject: str, session: requests.Session, mailto: str = None):
    params = {"search": subject, "per-page": 1}
    if mailto:
        params["mailto"] = mailto

    r = session.get(f"{OPENALEX_BASE}/topics", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("results"):
        return "topics.id", data["results"][0]["id"]

    r = session.get(f"{OPENALEX_BASE}/concepts", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("results"):
        return "concepts.id", data["results"][0]["id"]

    return None, None


def search_books_by_subject(subject, start_year=2021, end_year=2025, max_results=50, mailto=None):
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    key, id_url = resolve_subject_id(subject, session, mailto)
    if not key:
        return []

    params = {
        "filter": f"type:book,{key}:{id_url},publication_year:{start_year}-{end_year}",
        "per-page": 50,
    }
    if mailto:
        params["mailto"] = mailto

    all_rows, page = [], 1
    while len(all_rows) < max_results:
        params["page"] = page
        resp = session.get(f"{OPENALEX_BASE}/works", params=params, timeout=60)
        if resp.status_code == 429:
            time.sleep(2)
            continue
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            break

        for work in results:
            title = work.get("display_name") or "N/A"
            year = work.get("publication_year", "N/A")
            url = pick_best_url(work)
            authors = [
                (a.get("author") or {}).get("display_name", "")
                for a in work.get("authorships", [])
            ]
            all_rows.append(
                {
                    "Title": title,
                    "Authors": ", ".join([a for a in authors if a]),
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
    format: str = Query("json", description="Output format: json or csv"),
):
    subject_list = [s.strip() for s in subjects.split(",") if s.strip()]
    results = []

    for subject in subject_list:
        rows = search_books_by_subject(subject, start_year, end_year, max_results, mailto)
        results.extend(rows)

    if format == "csv":
        df = pd.DataFrame(results)
        buf = io.StringIO()
        df.to_csv(buf, index=False, encoding="utf-8")
        buf.seek(0)
        return StreamingResponse(
            io.BytesIO(buf.getvalue().encode("utf-8")),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=books.csv"},
        )

    return JSONResponse(content=results)
