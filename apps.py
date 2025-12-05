import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO

import streamlit as st
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.utils.dataframe import dataframe_to_rows
#hello

# ----------------------------------------------------------
# LOAD API KEYS FROM ST SECRETS (SAFE)
# ----------------------------------------------------------
SERP_API_KEY = st.secrets["SERP_API_KEY"]
ISBNDB_API_KEY = st.secrets["ISBNDB_API_KEY"]


# ----------------------------------------------------------
# COLOR MAP FOR EXCEL
# ----------------------------------------------------------
COLOR_MAP = {
    "serp": Font(color="1E3A8A"),
    "isbndb": Font(color="064E3B"),
    "google": Font(color="FACC15"),
}


# ----------------------------------------------------------
# AUTOSAVE INTO EXCEL (IN MEMORY)
# ----------------------------------------------------------
def save_partial_excel(results, color_results):
    wb = Workbook()
    ws = wb.active

    df_out = pd.DataFrame(results)

    for r_idx, row_data in enumerate(
        dataframe_to_rows(df_out, index=False, header=True), 1
    ):
        ws.append(row_data)

        if r_idx == 1:
            continue

        for c_idx, col_name in enumerate(df_out.columns, 1):
            src = color_results[r_idx - 2].get(col_name)
            if src in COLOR_MAP:
                ws.cell(r_idx, c_idx).font = COLOR_MAP[src]

    stream = BytesIO()
    wb.save(stream)
    stream.seek(0)
    return stream


# ----------------------------------------------------------
# HELPERS
# ----------------------------------------------------------
def is_missing(x):
    return x is None or str(x).strip() == ""


# ----------------------------------------------------------
# SERP FETCH
# ----------------------------------------------------------
def serp_fetch(isbn, domain):
    try:
        url = "https://serpapi.com/search.json"
        params = {
            "engine": "amazon",
            "amazon_domain": domain,
            "api_key": SERP_API_KEY,
            "k": isbn
        }

        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        results = data.get("organic_results", [])

        if not results:
            return {}

        first = results[0]
        price_field = first.get("price")
        price = price_field.get("raw") if isinstance(price_field, dict) else price_field

        return {
            "title": first.get("title"),
            "price": price
        }

    except:
        return {}


SERP_PRIORITY = ["amazon.in", "amazon.com", "amazon.co.uk", "amazon.de"]


def get_serp_sequential(isbn):
    final = {"title": None, "price": None}
    colors = {}
    domain_used = None
    serp_calls = 0

    for domain in SERP_PRIORITY:
        serp_calls += 1

        data = serp_fetch(isbn, domain)
        if not data:
            continue

        title = data.get("title")
        price = data.get("price")

        # RULE: Skip amazon.in if price is 0 or None
        if domain == "amazon.in" and (price is None or price == 0):
            continue

        if is_missing(final["title"]) and not is_missing(title):
            final["title"] = title
            colors["title"] = "serp"
            domain_used = domain

        if is_missing(final["price"]) and not is_missing(price) and price != 0:
            final["price"] = price
            colors["price"] = "serp"
            domain_used = domain

        if final["title"] and final["price"]:
            break

    return final, colors, domain_used, serp_calls


# ----------------------------------------------------------
# ISBNDB FETCH
# ----------------------------------------------------------
def get_isbndb_data(isbn):
    try:
        url = f"https://api2.isbndb.com/book/{isbn}"
        headers = {"Authorization": ISBNDB_API_KEY}
        r = requests.get(url, headers=headers, timeout=10)

        if r.status_code != 200:
            return {}, {}

        b = r.json().get("book", {})

        data = {
            "title": b.get("title"),
            "author": ", ".join(b.get("authors", [])),
            "publisher": b.get("publisher"),
            "binding": b.get("binding"),
            "edition": b.get("edition"),
            "number_of_pages": b.get("pages"),
            "category": None,
            "price": b.get("msrp")
        }

        return data, {k: "isbndb" for k, v in data.items() if not is_missing(v)}

    except:
        return {}, {}


# ----------------------------------------------------------
# GOOGLE BOOKS FETCH
# ----------------------------------------------------------
def get_google_books_data(isbn):
    try:
        url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
        r = requests.get(url, timeout=10)
        data = r.json()
    except:
        return {}, {}

    if "items" not in data:
        return {}, {}

    info = data["items"][0]["volumeInfo"]

    g = {
        "title": info.get("title"),
        "author": ", ".join(info.get("authors", [])) if info.get("authors") else None,
        "publisher": info.get("publisher"),
        "number_of_pages": info.get("pageCount"),
        "category": info.get("categories", [None])[0],
        "price": None
    }

    return g, {k: "google" for k, v in g.items() if not is_missing(v)}


# ----------------------------------------------------------
# PROCESS SINGLE ISBN
# ----------------------------------------------------------
def process_single_isbn(isbn):
    row = {
        "ISBN": isbn,
        "title": None,
        "author": None,
        "publisher": None,
        "binding": None,
        "edition": None,
        "number_of_pages": None,
        "category": None,
        "price": None,
        "amazon_domain_used": None,
        "serp_api_calls": None,
        "source_used": None
    }

    row_color = {"ISBN": None}

    # SERP
    serp_data, serp_colors, used_domain, serp_calls = get_serp_sequential(isbn)
    row["amazon_domain_used"] = used_domain
    row["serp_api_calls"] = serp_calls

    for k, v in serp_data.items():
        if not is_missing(v):
            row[k] = v
            row_color[k] = serp_colors.get(k)

    # ISBNDB
    isbndb_data, c2 = get_isbndb_data(isbn)
    for k, v in isbndb_data.items():
        if is_missing(row[k]) and not is_missing(v):
            row[k] = v
            row_color[k] = c2.get(k)

    # Google Books
    google_data, c3 = get_google_books_data(isbn)
    for k, v in google_data.items():
        if is_missing(row[k]) and not is_missing(v):
            row[k] = v
            row_color[k] = c3.get(k)

    used_sources = set([v for v in row_color.values() if v])
    row["source_used"] = ", ".join(sorted(used_sources)) if used_sources else None

    return row, row_color


# ----------------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------------
st.title("📚 ISBN Data Extraction Tool")
st.write("Upload an Excel file containing an **ISBN** column.")

uploaded = st.file_uploader("Upload Excel", type=["xlsx"])

if uploaded:
    df = pd.read_excel(uploaded)
    st.write("### Preview Input File:")
    st.dataframe(df.head())

    if st.button("🚀 Start Processing"):
        results = []
        color_results = []

        progress = st.progress(0)
        status = st.empty()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(process_single_isbn, isbn): isbn for isbn in df["ISBN"]}

            for i, future in enumerate(as_completed(futures), 1):
                row, row_color = future.result()
                results.append(row)
                color_results.append(row_color)

                progress.progress(i / len(df))
                status.write(f"Processed {i}/{len(df)} ISBNs")

        # Final Excel
        excel_data = save_partial_excel(results, color_results)

        st.success("Processing complete!")

        st.download_button(
            "⬇ Download Output Excel",
            data=excel_data,
            file_name="isbn_output.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
