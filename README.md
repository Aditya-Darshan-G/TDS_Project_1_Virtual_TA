---
title: TDS Semantic Search App
emoji: 🔍
colorFrom: blue
colorTo: indigo
sdk: docker
app_file: app/main.py
pinned: false
---

# Gemini Semantic Search App

This is a FastAPI-based semantic search app using Gemini embeddings. Deployable via Docker or Hugging Face Spaces.

### Endpoints

- `POST /query` – Ask questions using Gemini + semantic search.
- `GET /health` – Check if embeddings are loaded.
