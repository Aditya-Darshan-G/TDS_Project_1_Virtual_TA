# chunks_to_embeddings.py

import os
import sqlite3
import numpy as np
import json
import time
import requests
import base64
from dotenv import load_dotenv
from google import generativeai as genai
from tqdm import tqdm

# Load environment variables
load_dotenv()
GENAI_API_KEY = os.getenv("GENAI_API_KEY")
if not GENAI_API_KEY:
    raise ValueError("Missing GENAI_API_KEY in .env file")

# Configure Gemini
genai.configure(api_key=GENAI_API_KEY)
embedding_model = genai.GenerativeModel("gemini-embedding-model")
caption_model = genai.GenerativeModel("gemini-1.5-flash")

DB_PATH = "data/knowledge_base.db"
OUTPUT_FILE = "data/embeddings.npz"

# Rate limiter for Gemini (max 2 requests/sec or 60/min)
class RateLimiter:
    def __init__(self, rpm=60, rps=2):
        self.rpm = rpm
        self.rps = rps
        self.timestamps = []
        self.last_time = 0

    def wait(self):
        now = time.time()
        delta = now - self.last_time
        if delta < 1.0 / self.rps:
            time.sleep((1.0 / self.rps) - delta)
        self.last_time = time.time()

        self.timestamps = [t for t in self.timestamps if now - t < 60]
        if len(self.timestamps) >= self.rpm:
            wait_time = 60 - (now - self.timestamps[0])
            time.sleep(wait_time)
            self.timestamps = [t for t in self.timestamps if time.time() - t < 60]

        self.timestamps.append(time.time())

rate_limiter = RateLimiter()

# Generate embedding for a text chunk
def get_embedding(text, max_retries=3):
    for attempt in range(max_retries):
        try:
            rate_limiter.wait()
            response = genai.embed_content(
                model="models/embedding-001",
                content=text,
                task_type="retrieval_document"
            )
            return response["embedding"]
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"[!] Failed after {max_retries} attempts: {e}")
                return None
            wait = 2 ** attempt
            print(f"Retrying after {wait} seconds due to error: {e}")
            time.sleep(wait)
    return None

# Captioning for image URLs
def get_caption_from_url(url, max_retries=2):
    CAPTION_PROMPT = (
        "Provide a detailed factual description of the image. List all visible text, diagrams, "
        "charts, labels, and objects, including their spatial layout and relationships. Focus only "
        "on what can be directly seen, avoiding interpretation or assumptions. Describe every "
        "element as if preparing the image for a blind person to understand its structure and content."
    )

    for attempt in range(max_retries):
        try:
            rate_limiter.wait()

            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(f"Failed to download image: {url}")

            image_bytes = response.content
            mime_type = response.headers.get("Content-Type", "image/webp")  # Fallback

            gemini_response = caption_model.generate_content([
                {
                    "inline_data": {
                        "mime_type": mime_type,
                        "data": base64.b64encode(image_bytes).decode("utf-8")
                    }
                },
                CAPTION_PROMPT
            ])

            return gemini_response.text.strip()

        except Exception as e:
            print(f"[Captioning] Attempt {attempt+1} failed for {url}: {e}")
            time.sleep(2 ** attempt)

    return None

# Fetch (content, source_url) from markdown and discourse chunks
def get_text_chunks_from_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT content, source_url FROM markdown_chunks")
    markdown_chunks = [(row["content"], row["source_url"]) for row in cursor.fetchall()]

    cursor.execute("SELECT content, source_url FROM discourse_chunks")
    discourse_chunks = [(row["content"], row["source_url"]) for row in cursor.fetchall()]

    conn.close()
    return markdown_chunks + discourse_chunks

# Fetch image URLs from DB
def get_image_urls_from_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT image_url FROM image_chunks")
        image_urls = [row["image_url"] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"[!] Warning: No image_chunks table or query error: {e}")
        image_urls = []

    conn.close()
    return image_urls

# Main embedding pipeline
def main():
    text_chunks = get_text_chunks_from_db()
    image_urls = get_image_urls_from_db()

    embeddings = []
    valid_chunks = []
    source_urls = []

    # Text embeddings
    for content, url in tqdm(text_chunks, desc="Embedding text chunks"):
        embedding = get_embedding(content)
        if embedding:
            embeddings.append(embedding)
            valid_chunks.append(content)
            source_urls.append(url)

    # Image embeddings
    for url in tqdm(image_urls, desc="Captioning and embedding images"):
        caption = get_caption_from_url(url)
        if caption:
            embedding = get_embedding(caption)
            if embedding:
                embeddings.append(embedding)
                valid_chunks.append(f"[IMAGE] {caption}")
                source_urls.append(url)

    # Save all to .npz
    np.savez(OUTPUT_FILE, chunks=valid_chunks, embeddings=embeddings, source_urls=source_urls)
    print(f"✅ Saved {len(valid_chunks)} embeddings with source URLs to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
