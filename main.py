import os
import re
import time
import logging
import json
import tkinter as tk
from tkinter import ttk
from urllib.parse import quote
import concurrent.futures

import requests
import psycopg2
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from markdownify import markdownify as md
from psycopg2.extras import execute_batch

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


# --- Helper Functions ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Error connecting to the database: {e}")
        return None


def execute_query(query, params=None, fetch=None):
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


# --- Text and HTML Processing Functions ---

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


# --- Core Scraping Workflow Functions ---
def search_scribd_for_urls(query, date_filter):
    logging.info(f"Starting Scribd search for query: '{query}' with filter: '{date_filter}'")
    document_urls = set()

    for page_num in range(1, 1000):
        try:
            encoded_query = quote(query)

            # Dynamically create the filter part of the URL
            filter_json = f'{{"date_uploaded":"{date_filter}"}}'
            encoded_filter = quote(filter_json)

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
    """
    Efficiently stores URLs in the database using a single transaction and batch insert,
    then fetches the URLs for newly added documents.
    """
    logging.info(f"Processing {len(urls)} URLs for keyword '{query}'...")
    if not urls:
        return []

    conn = None
    new_document_urls = []

    try:
        # 1. Establish a single, persistent connection for all operations.
        conn = get_db_connection()
        with conn.cursor() as cur:

            # 2. Use a highly efficient batch insert instead of a slow loop.
            sql_insert = """
                INSERT INTO documents_deneme (url, search_keyword, source, first_seen, last_seen)
                VALUES (%s, %s, 'scribd', NOW(), NOW())
                ON CONFLICT (url) DO UPDATE
                SET last_seen = NOW();
            """
            data_to_insert = [(url, query) for url in urls]
            execute_batch(cur, sql_insert, data_to_insert)
            logging.info(f"Batch insert of {len(urls)} URLs complete.")

            # 3. Commit the transaction. This makes all the above inserts permanent
            # and guarantees they are visible to the next query.
            conn.commit()
            logging.info("Transaction committed successfully.")

            # 4. Now we can safely query for the new documents. The sleep calls are gone.
            sql_check_new = """
                SELECT url FROM documents_deneme
                WHERE search_keyword = %s AND contents IS NULL;
            """
            cur.execute(sql_check_new, (query,))
            new_docs_raw = cur.fetchall()
            new_document_urls = [row[0] for row in new_docs_raw] if new_docs_raw else []

            logging.info(f"Found {len(new_document_urls)} new documents to scrape.")

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Database transaction failed: {error}")
        if conn:
            conn.rollback()  # If anything goes wrong, undo all changes.
    finally:
        if conn:
            conn.close()  # 5. Always close the connection when done.

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
        UPDATE documents_deneme SET
            title = %s,
            description = %s,
            uploader = %s,
            contents = %s
        WHERE url = %s;
    """
    params = (
        data.get('title'),
        data.get('description'),
        data.get('uploader'),
        data.get('contents'),
        data.get('url')
    )
    execute_query(sql, params)
    logging.info(f"Successfully updated document in DB: {data.get('url')}")


def highlight_keywords_in_db(query):
    # 1. Clean the query string by removing surrounding quotes
    # .strip('"') removes leading and trailing double quotes if they exist.
    highlight_term = query.strip('"')

    logging.info(f"Highlighting keyword '{highlight_term}' in the database.")

    # 2. Use the cleaned term for both the regex pattern and the replacement string
    # Escape the term to handle special characters correctly in the regex
    # The pattern now searches for the unquoted term
    pattern = f"\\y{re.escape(highlight_term)}\\y"
    replacement = f"<mark>{highlight_term}</mark>"

    # 3. The SQL query remains the same, but it uses the cleaned pattern and replacement
    sql = """
        UPDATE documents_deneme
        SET highlighted_contents = REGEXP_REPLACE(contents, %s, %s, 'gi')
        WHERE search_keyword ILIKE %s;
    """
    # The last %s parameter still uses the original, potentially quoted query to target
    # documents that were scraped under that specific search query.
    execute_query(sql, (pattern, replacement, f"%{query}%"))
    logging.info("Keyword highlighting complete.")


# --- Main Workflow Orchestration ---
# --- Main Workflow Orchestration ---
def run_workflow(query, date_filter):
    # ADD THIS CONSTANT AT THE TOP OF THE FUNCTION OR SCRIPT
    DOCUMENT_PROCESS_LIMIT = 1

    try:
        print(f"🚀 Starting process for query: '{query}' with filter '{date_filter}'")
        doc_urls = search_scribd_for_urls(query, date_filter)
        if not doc_urls:
            print(f"⏹️ No document URLs found for '{query}'. Stopping.")
            return
        print(f"✅ Found {len(doc_urls)} document URLs...")

        new_urls_to_scrape = process_and_store_urls(doc_urls, query)

        # --- NEW LOGIC TO LIMIT DOCUMENT PROCESSING ---
        if len(new_urls_to_scrape) > DOCUMENT_PROCESS_LIMIT:
            print(
                f"⚠️ Found {len(new_urls_to_scrape)} new documents. Limiting processing to the first {DOCUMENT_PROCESS_LIMIT}.")
            new_urls_to_scrape = new_urls_to_scrape[:DOCUMENT_PROCESS_LIMIT]
        # --- END OF NEW LOGIC ---

        if not new_urls_to_scrape:
            print("✅ No new content to scrape.")
        else:
            print(f"Scraping content for {len(new_urls_to_scrape)} new documents...")

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(scrape_document_details, new_urls_to_scrape))

            print("✅ Scraping complete. Updating database...")
            for scraped_data in results:
                if scraped_data:
                    update_document_in_db(scraped_data)

        print("✨ Highlighting keywords in the database...")
        highlight_keywords_in_db(query)
        print(f"\n🎉 Process for query '{query}' is complete!")
    except Exception as e:
        logging.error(f"A critical error occurred during the workflow: {e}")
        print(f"❌ An error occurred: {e}")


from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/scrape', methods=['POST'])
def scrape_api():
    data = request.get_json()
    keyword = data.get('keyword')
    date_filter = data.get('date_filter')
    if not keyword or not date_filter:
        return jsonify({'error': 'Missing keyword or date_filter'}), 400

    try:
        run_workflow(keyword, date_filter)
        # Fetch processed documents from DB (example: those with non-null contents)
        sql = "SELECT url, title, description, uploader, contents, highlighted_contents FROM documents_deneme WHERE search_keyword = %s AND contents IS NOT NULL;"
        results = execute_query(sql, (keyword,), fetch="all")
        docs = [
            {
                "url": r[0],
                "title": r[1],
                "description": r[2],
                "uploader": r[3],
                "contents": r[4],
                "highlighted_contents": r[5]
            }
            for r in results or []
        ]
        return jsonify({'documents': docs})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == "__main__":
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
        logging.error("One or more required database environment variables are not set. Please check your .env file.")
    else:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
