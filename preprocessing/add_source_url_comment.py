import os
import re

BASE_DIR = "data/tools-in-data-science-public"
BASE_URL = "https://tds.s-anand.net/#/"

def generate_page_url(md_path):
    filename = os.path.splitext(os.path.basename(md_path))[0]
    page_name = filename.replace("-", " ").replace("_", " ").title().replace(" ", "-").lower()
    return f"{BASE_URL}{page_name}"

def normalize_url_comment_line(url):
    return f"<!-- source_url: {url} -->"

def patch_markdown(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Generate the intended URL
    url = generate_page_url(filepath)
    new_comment_line = normalize_url_comment_line(url)

    # Check if the first line is a source_url comment
    if lines and lines[0].strip().startswith("<!-- source_url:"):
        lines[0] = new_comment_line + "\n"
        print(f"🔁 Updated comment in: {filepath}")
    else:
        lines = [new_comment_line + "\n", "\n"] + lines
        print(f"➕ Inserted comment in: {filepath}")

    with open(filepath, "w", encoding="utf-8") as f:
        f.writelines(lines)

def process_all_markdown_files():
    for root, _, files in os.walk(BASE_DIR):
        for file in files:
            if file.endswith(".md"):
                patch_markdown(os.path.join(root, file))

if __name__ == "__main__":
    process_all_markdown_files()
