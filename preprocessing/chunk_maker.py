import os
import json
import sqlite3
import re
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm

# Paths
DISCOURSE_DIR = "data/discourse_posts"
MARKDOWN_DIR = "data/tools-in-data-science-public"
DB_PATH = "data/knowledge_base.db"

# Chunking parameters
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200

# Ensure directories exist
os.makedirs(DISCOURSE_DIR, exist_ok=True)
os.makedirs(MARKDOWN_DIR, exist_ok=True)

# Create DB connection and tables
def create_connection():
    return sqlite3.connect(DB_PATH)

def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS discourse_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER,
            topic_id INTEGER,
            chunk_index INTEGER,
            content TEXT,
            source_url TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS markdown_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT,
            chunk_index INTEGER,
            content TEXT,
            source_url TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS image_chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT,
            image_url TEXT
        )
    ''')
    conn.commit()

def clean_html(raw_html):
    soup = BeautifulSoup(raw_html, "html.parser")
    for script_or_style in soup(["script", "style"]):
        script_or_style.decompose()
    text = soup.get_text(separator=" ")
    return re.sub(r'\s+', ' ', text).strip()

def create_chunks(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    if not text:
        return []
    text = re.sub(r'\s+', ' ', text).strip()
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks

def process_discourse_json(conn):
    cursor = conn.cursor()
    json_files = list(Path(DISCOURSE_DIR).glob("*.json"))
    total_chunks = 0

    for file_path in tqdm(json_files, desc="Discourse JSON"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                wrapped_data = json.load(f)
                data = wrapped_data["post_data"]
                topic_id = data.get("id", -1)
                slug = data.get("slug", "")
                posts = data.get("post_stream", {}).get("posts", [])
        except (json.JSONDecodeError, KeyError):
            print(f"Skipping malformed JSON: {file_path}")
            continue

        base_url = "https://discourse.onlinedegree.iitm.ac.in"

        for post in posts:
            post_id = post.get("id")
            raw_html = post.get("cooked", "")
            cleaned_text = clean_html(raw_html)
            if len(cleaned_text) < 20:
                continue
            chunks = create_chunks(cleaned_text)
            for i, chunk in enumerate(chunks):
                source_url = f"{base_url}/t/{slug}/{topic_id}/{i}"
                cursor.execute("""
                    INSERT INTO discourse_chunks (post_id, topic_id, chunk_index, content, source_url)
                    VALUES (?, ?, ?, ?, ?)
                """, (post_id, topic_id, i, chunk, source_url))
                total_chunks += 1
    conn.commit()
    print(f"✅ Finished processing discourse. Created {total_chunks} chunks.")

def process_markdown_files(conn):
    cursor = conn.cursor()
    markdown_files = list(Path(MARKDOWN_DIR).rglob("*.md"))
    total_chunks = 0
    total_image_urls = 0

    for file_path in tqdm(markdown_files, desc="Markdown Files"):
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # Extract source URL from HTML comment
        match = re.search(r'<!--\s*source_url:\s*(.*?)\s*-->', text)
        source_url = match.group(1).strip() if match else ""

        # Extract image URLs
        image_urls = re.findall(r'!\[.*?\]\((.*?)\)', text)
        for url in image_urls:
            cursor.execute("""
                INSERT INTO image_chunks (file_path, image_url)
                VALUES (?, ?)
            """, (str(file_path), url))
        total_image_urls += len(image_urls)

        # Clean text by removing Markdown formatting
        cleaned_text = re.sub(r'`{1,3}.*?`{1,3}', '', text, flags=re.DOTALL)  # code blocks
        cleaned_text = re.sub(r'#+\s*', '', cleaned_text)  # headers
        cleaned_text = re.sub(r'\[.*?\]\(.*?\)', '', cleaned_text)  # links
        cleaned_text = re.sub(r'!\[.*?\]\(.*?\)', '', cleaned_text)  # images
        cleaned_text = re.sub(r'\*\*|__|\*|_', '', cleaned_text)  # bold/italic
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

        chunks = create_chunks(cleaned_text)
        for i, chunk in enumerate(chunks):
            cursor.execute("""
                INSERT INTO markdown_chunks (file_path, chunk_index, content, source_url)
                VALUES (?, ?, ?, ?)
            """, (str(file_path), i, chunk, source_url))
            total_chunks += 1

    conn.commit()
    print(f"✅ Finished processing markdown. Created {total_chunks} chunks and {total_image_urls} image URLs.")

if __name__ == "__main__":
    conn = create_connection()
    create_tables(conn)
    process_discourse_json(conn)
    process_markdown_files(conn)
    conn.close()
