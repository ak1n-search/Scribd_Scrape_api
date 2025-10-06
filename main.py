import os
import re
import time
import logging
import json
import datetime
from urllib.parse import quote
import concurrent.futures
from typing import Optional, List

import requests
import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from markdownify import markdownify as md
from psycopg2.extras import execute_batch, DictCursor
from fastapi import FastAPI, BackgroundTasks, HTTPException, Query
from pydantic import BaseModel, Field

# --- Configuration and Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_PORT = os.environ.get("DB_PORT", "5432")

# --- Use a Session object for performance ---
session = requests.Session()

# --- FastAPI App Initialization ---
app = FastAPI(
    title="Scribd Scraper API",
    description="An API to trigger a Scribd scraper and retrieve the results from a database.",
    version="1.0.0"
)


# --- Pydantic Models for API Data Validation ---
class ScrapeRequest(BaseModel):
    query: str = Field(..., example="financial report 2024", description="The search term for Scribd.")
    date_filter: str = Field(..., example="ct_lang=0&filters=%7B\"date_uploaded\"%3A\"6month\"%7D",
                             description="The URL-encoded date filter string for the search.")


# --- Helper Functions ---
def get_db_connection(cursor_factory=None):
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            cursor_factory=cursor_factory
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to the database: {e}")
        return None


def execute_query(query, params=None, fetch=None):
    """Executes a single database query."""
    conn = get_db_connection()
    if not conn:
        return None

    results = None
    try:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if fetch == "one":
                results = cur.fetchone()
            elif fetch == "all":
                results = cur.fetchall()
            conn.commit()
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        conn.rollback()
    finally:
        conn.close()
    return results


# --- Text and HTML Processing Functions (Your Original Code) ---

def parse_jsonp_content(jsonp_content):
    if not jsonp_content or not isinstance(jsonp_content, str):
        return ""
    match = re.search(r'window\.page\d+_callback\((.*)\);', jsonp_content, re.DOTALL)
    if not match:
        return ""
    json_string = match.group(1)
    try:
        data_array = json.loads(json_string)
        if not isinstance(data_array, list) or not data_array:
            return ""
        html_content = data_array[0]
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text()
        text = text.replace(u'\xa0', ' ')
        cleaned_text = re.sub(r'\s+', ' ', text).strip()
        return cleaned_text
    except (json.JSONDecodeError, IndexError) as e:
        logging.warning(f"Could not parse JSONP content: {e}")
        return ""


def clean_markdown(markdown_text):
    cleaned = re.sub(r'\s*## Share this document[\s\S]*', '', markdown_text, flags=re.IGNORECASE)
    cleaned = re.sub(r'^[\s\S]*?(?=Full description)', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def parse_title_desc_uploader(cleaned_markdown):
    data = {"title": None, "description": None, "uploader": None}
    title_match = re.search(r'SaveSave\s+(.*?)\s+For Later', cleaned_markdown, re.IGNORECASE)
    if title_match:
        data["title"] = title_match.group(1).strip()

    if data["title"]:
        try:
            escaped_title = re.escape(data["title"])
            desc_pattern = re.compile(f'#\\s*{escaped_title}\\s*(.*?)\\s*Read more', re.DOTALL | re.IGNORECASE)
            desc_match = desc_pattern.search(cleaned_markdown)
            if desc_match:
                data["description"] = desc_match.group(1).strip()
        except re.error as e:
            logging.error(f"Regex error for description: {e}")

    uploader_match = re.search(r'Uploaded by \[(.*?)\]\((.*?)\)', cleaned_markdown, re.IGNORECASE)
    if uploader_match:
        name = uploader_match.group(1)
        link = uploader_match.group(2)
        data["uploader"] = f"{name} ({link})"

    return data


# --- Core Scraping Workflow Functions (Your Original Code) ---

def search_scribd_for_urls(query, date_filter):
    logging.info(f"Starting Scribd search for query: '{query}' with filter: '{date_filter}'")
    document_urls = set()

    for page_num in range(1, 1000):
        try:
            encoded_query = quote(query)
            search_url = (
                f"https://www.scribd.com/search/query?query={encoded_query}"
                f"&content_type=documents&page={page_num}&verbatim=true"
                f"{date_filter}"
            )
            response = session.get(search_url, timeout=20, headers={'Accept': 'application/json'})
            time.sleep(3)
            response.raise_for_status()
            data = response.json()
            docs = data.get("results", {}).get("documents", {}).get("content", {}).get("documents", [])

            if not docs:
                logging.info(f"No more results found on page {page_num}. Ending search.")
                break

            for doc in docs:
                url = doc.get('reader_url')
                if url and isinstance(url, str):
                    document_urls.add(url)

            logging.info(f"Page {page_num}: Found {len(docs)} docs. Total unique URLs: {len(document_urls)}")

        except requests.RequestException as e:
            logging.error(f"HTTP Request failed for page {page_num}: {e}")
        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON from response on page {page_num}.")
        except Exception as e:
            logging.error(f"An error occurred while processing page {page_num}: {e}")
    return list(document_urls)


def process_and_store_urls(urls, query):
    logging.info(f"Processing {len(urls)} URLs for keyword '{query}'...")
    if not urls:
        return []

    conn = None
    new_document_urls = []

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            sql_insert = """
                         INSERT INTO documents_deneme (url, search_keyword, source, first_seen, last_seen)
                         VALUES (%s, %s, 'scribd', NOW(), NOW()) ON CONFLICT (url) DO \
                         UPDATE \
                             SET last_seen = NOW(); \
                         """
            data_to_insert = [(url, query) for url in urls]
            execute_batch(cur, sql_insert, data_to_insert)
            logging.info(f"Batch insert of {len(urls)} URLs complete.")

            conn.commit()
            logging.info("Transaction committed successfully.")

            sql_check_new = """
                            SELECT url \
                            FROM documents_deneme
                            WHERE search_keyword = %s \
                              AND contents IS NULL; \
                            """
            cur.execute(sql_check_new, (query,))
            new_docs_raw = cur.fetchall()
            new_document_urls = [row[0] for row in new_docs_raw] if new_docs_raw else []
            logging.info(f"Found {len(new_document_urls)} new documents to scrape.")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Database transaction failed: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
    return new_document_urls


def scrape_document_details(doc_url):
    logging.info(f"Scraping document: {doc_url}")
    try:
        response = session.get(doc_url, timeout=30)
        response.raise_for_status()
        html_content = response.text

        markdown_text = md(html_content, heading_style="ATX")
        cleaned_markdown = clean_markdown(markdown_text)
        main_page_data = parse_title_desc_uploader(cleaned_markdown)

        dynamic_urls = re.findall(r'contentUrl["\']?\s*:\s*["\'](https?://[^"\']+)', html_content)

        all_contents = []
        for i, url in enumerate(dynamic_urls[:200]):
            try:
                content_resp = session.get(url, timeout=20)
                extracted_text = parse_jsonp_content(content_resp.text)
                if extracted_text:
                    all_contents.append(extracted_text)
            except requests.RequestException as e:
                logging.warning(f"Could not fetch dynamic content from {url}: {e}")

        final_text = " \n\n- - -\n\n ".join(all_contents)

        scraped_data = {
            "url": doc_url,
            "title": main_page_data.get("title"),
            "description": main_page_data.get("description"),
            "uploader": main_page_data.get("uploader"),
            "contents": final_text,
        }
        return scraped_data

    except requests.RequestException as e:
        logging.error(f"Failed to get HTML for {doc_url}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while scraping {doc_url}: {e}")
        return None


def update_document_in_db(data):
    if not data or not data.get('url'):
        logging.warning("Skipping database update due to empty data.")
        return

    sql = """
          UPDATE documents_deneme \
          SET title       = %s, \
              description = %s, \
              uploader    = %s, \
              contents    = %s
          WHERE url = %s; \
          """
    params = (data.get('title'), data.get('description'), data.get('uploader'), data.get('contents'), data.get('url'))
    execute_query(sql, params)
    logging.info(f"Successfully updated document in DB: {data.get('url')}")


def highlight_keywords_in_db(query):
    highlight_term = query.strip('"')
    logging.info(f"Highlighting keyword '{highlight_term}' in the database.")
    pattern = f"\\y{re.escape(highlight_term)}\\y"
    replacement = f"<mark>{highlight_term}</mark>"
    sql = """
          UPDATE documents_deneme
          SET highlighted_contents = REGEXP_REPLACE(contents, %s, %s, 'gi')
          WHERE search_keyword ILIKE %s; \
          """
    execute_query(sql, (pattern, replacement, f"%{query}%"))
    logging.info("Keyword highlighting complete.")


# --- Main Workflow Orchestration (Your Original Code) ---
def run_workflow(query, date_filter):
    DOCUMENT_PROCESS_LIMIT = 1  # You can adjust this limit

    try:
        logging.info(f"🚀 Starting process for query: '{query}' with filter '{date_filter}'")
        doc_urls = search_scribd_for_urls(query, date_filter)
        if not doc_urls:
            logging.info(f"⏹️ No document URLs found for '{query}'. Stopping.")
            return
        logging.info(f"✅ Found {len(doc_urls)} document URLs...")

        new_urls_to_scrape = process_and_store_urls(doc_urls, query)

        if len(new_urls_to_scrape) > DOCUMENT_PROCESS_LIMIT:
            logging.warning(
                f"⚠️ Found {len(new_urls_to_scrape)} new documents. Limiting processing to the first {DOCUMENT_PROCESS_LIMIT}.")
            new_urls_to_scrape = new_urls_to_scrape[:DOCUMENT_PROCESS_LIMIT]

        if not new_urls_to_scrape:
            logging.info("✅ No new content to scrape.")
        else:
            logging.info(f"Scraping content for {len(new_urls_to_scrape)} new documents...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(scrape_document_details, new_urls_to_scrape))

            logging.info("✅ Scraping complete. Updating database...")
            for scraped_data in results:
                if scraped_data:
                    update_document_in_db(scraped_data)

        logging.info("✨ Highlighting keywords in the database...")
        highlight_keywords_in_db(query)
        logging.info(f"\n🎉 Process for query '{query}' is complete!")
    except Exception as e:
        logging.error(f"A critical error occurred during the workflow: {e}")


# --- API Endpoints ---

@app.get("/", summary="Health Check")
def read_root():
    """A simple health check endpoint to confirm the API is running."""
    return {"status": "ok", "message": "Welcome to the Scraper API!"}


@app.post("/scrape", status_code=202, summary="Start Scraping Job")
def trigger_scraping_job(request: ScrapeRequest, background_tasks: BackgroundTasks):
def trigger_scraping_job(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Accepts a search query and date filter, then starts the scraping process as a background task.
    """
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        raise HTTPException(status_code=500, detail="Database environment variables are not configured on the server.")

    logging.info(f"Received scraping request for query: '{request.query}'")
    background_tasks.add_task(run_workflow, request.query, request.date_filter)
    return {"message": "Scraping job accepted and started in the background."}


@app.get("/documents", summary="Retrieve Scraped Documents")
def get_documents(
        keyword: Optional[str] = Query(None, description="Filter documents by the original search keyword."),
        limit: int = Query(20, gt=0, le=100, description="The number of documents to return."),
        offset: int = Query(0, ge=0, description="The starting offset for pagination.")
):
    """
    Retrieves scraped documents from the database with optional filtering by keyword and pagination.
    """
    conn = get_db_connection(cursor_factory=DictCursor)
    if not conn:
        raise HTTPException(status_code=503, detail="Database connection could not be established.")

    try:
        with conn.cursor() as cur:
            if keyword:
                query = "SELECT * FROM documents_deneme WHERE search_keyword ILIKE %s ORDER BY last_seen DESC LIMIT %s OFFSET %s;"
                params = (f"%{keyword}%", limit, offset)
            else:
                query = "SELECT * FROM documents_deneme ORDER BY last_seen DESC LIMIT %s OFFSET %s;"
                params = (limit, offset)

            cur.execute(query, params)
            results = cur.fetchall()

            # Convert DictRow objects to plain dicts and format datetimes for JSON compatibility
            json_results = []
            for row in results:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    if isinstance(value, datetime.datetime):
                        row_dict[key] = value.isoformat()
                json_results.append(row_dict)
            return json_results

    except Exception as e:
        logging.error(f"Failed to fetch documents: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve documents from the database.")
    finally:
        if conn:
            conn.close()