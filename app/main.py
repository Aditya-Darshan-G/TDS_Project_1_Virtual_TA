from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
import os
import numpy as np
import re
import logging
from dotenv import load_dotenv
import asyncio
from google import generativeai as genai

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("GENAI_API_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
EMBEDDING_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "embeddings.npz")
SIMILARITY_THRESHOLD = 0.68
MAX_RESULTS = 10
MAX_CONTEXT_CHUNKS = 4

# Load Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-1.5-flash")

# Load embeddings
try:
    data = np.load(EMBEDDING_FILE, allow_pickle=True)
    chunks = data["chunks"]
    embeddings = data["embeddings"]
    source_urls = data["source_urls"]
except Exception as e:
    logger.error("Failed to load embeddings: %s", e)
    raise RuntimeError("Failed to load embeddings from disk")

# App setup
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Schemas
class QueryRequest(BaseModel):
    question: str
    image: Optional[str] = None

class LinkInfo(BaseModel):
    url: str
    text: str

class QueryResponse(BaseModel):
    answer: str
    links: List[LinkInfo]

# Cosine similarity
def cosine_similarity(a, b):
    a, b = np.array(a), np.array(b)
    if np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
        return 0.0
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

# Embed text
async def embed_text(query: str):
    if not GEMINI_API_KEY:
        logger.error("GENAI_API_KEY is missing")
        raise HTTPException(status_code=500, detail="GENAI_API_KEY missing")

    try:
        logger.info("Calling Gemini embed_content...")
        response = genai.embed_content(
            model="models/embedding-001",
            content=query,
            task_type="semantic_similarity"
        )
        logger.info("Embedding response received")
        return response["embedding"]
    except Exception as e:
        logger.error("Embedding failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Embedding failed: {e}")


# Search
def search_similar_chunks(query_emb):
    scores = [cosine_similarity(query_emb, e) for e in embeddings]
    sorted_indices = np.argsort(scores)[::-1]
    seen_sources = set()
    results = []

    for idx in sorted_indices:
        if scores[idx] < SIMILARITY_THRESHOLD:
            continue
        url = source_urls[idx]
        if url not in seen_sources:
            seen_sources.add(url)
            results.append({
                "url": url,
                "text": chunks[idx],
                "score": float(scores[idx])
            })
        if len(results) >= MAX_RESULTS:
            break
    return results

# Prompt builder
def build_prompt(question: str, context_snippets: List[dict]) -> str:
    context = "\n\n".join([f"Source: {c['url']}\n{c['text']}" for c in context_snippets])
    return f"""
You are an expert-level teaching assistant specializing in Data Science. Your responses must be concise, accurate, and strictly grounded in the provided sources.

For every factual claim you make, include a citation from the sources. Do not refer to any information that is not directly cited. Every citation you use in the answer must also appear in the final “Sources” list, and vice versa. This is essential.

Use the exact following format for each source in the list:
Source: <URL>, Text: "<relevant quote or explanation from the source>"

Do not use parentheses or other formats. This format will be machine-read for URL extraction.

---

Question:
{question}

---

Sources:
{context}

---

Respond with:
Answer:
<your detailed answer using the sources>

Sources:
1. Source: <url_1>, Text: "<quote or supporting explanation>"
2. Source: <url_2>, Text: "<quote or supporting explanation>"
"""

# Generate answer
async def generate_answer(question: str, context_snippets: List[dict]):
    prompt = build_prompt(question, context_snippets)
    try:
        result = model.generate_content(prompt)
        return result.text.strip()
    except Exception as e:
        logger.error("LLM generation failed: %s", e)
        raise HTTPException(status_code=500, detail="Failed to generate answer")

# Parse output
def extract_answer_and_links(text):
    parts = text.split("Sources:", 1)
    answer = parts[0].strip()
    links = []
    if len(parts) > 1:
        for line in parts[1].strip().split("\n"):
            # Match either 'URL:' or 'Source:' prefix
            match = re.search(r'(?:URL|Source):\s*(\S+),\s*Text:\s*["“”]?(.*?)["“”]?\s*$', line)
            if match:
                url, snippet = match.groups()
                links.append({"url": url.strip(), "text": snippet.strip()})

    # Fallback: extract any URL inside the answer body
    if not links:
        urls = re.findall(r'https?://\S+', answer)
        for url in urls:
            links.append({"url": url, "text": "Link referenced in answer."})

    return {"answer": answer, "links": links}


    return {"answer": answer, "links": links}

# API routes
@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    query_emb = await embed_text(request.question)
    top_chunks = search_similar_chunks(query_emb)
    if not top_chunks:
        return QueryResponse(answer="No relevant information found.", links=[])
    raw_output = await generate_answer(request.question, top_chunks)
    parsed = extract_answer_and_links(raw_output)
    return QueryResponse(answer=parsed["answer"], links=parsed["links"])

@app.get("/health")
def health():
    return {"status": "ok", "chunks": len(chunks), "embeddings": len(embeddings)}

@app.get("/")
def read_root():
    return {"message": "TDS Semantic Search App is running!"}

# Local dev run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
