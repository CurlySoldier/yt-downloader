import os
import uuid
import threading
from flask import Flask, request, jsonify, send_from_directory, send_file
import yt_dlp

app = Flask(__name__, static_folder="static")

# --- Config (override via environment variables) ---
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", os.path.expanduser("~/downloads"))
OUTPUT_TEMPLATE = os.environ.get(
    "OUTPUT_TEMPLATE",
    "%(playlist_title)s/%(playlist_index)s - %(title)s.%(ext)s",
)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# In-memory task store
tasks = {}


# ---------------------------------------------------------------------------
# yt-dlp option factories
# ---------------------------------------------------------------------------

def _info_opts():
    """Lightweight opts for metadata-only fetches."""
    return {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
    }


def _download_opts(progress_hook):
    """Full download opts matching the original CLI flags."""
    return {
        # Output path
        "outtmpl": os.path.join(DOWNLOAD_DIR, OUTPUT_TEMPLATE),

        # Audio
        "format": "bestaudio/best",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "0",   # VBR best quality (matches --audio-quality 0)
            },
            {"key": "FFmpegMetadata",  "add_metadata": True},   # --embed-metadata
            {"key": "EmbedThumbnail"},                           # --embed-thumbnail
        ],

        # ID3 tag mapping  (mirrors original parse_metadata flags)
        "parse_metadata": [
            "playlist_title:%(album)s",
            "title:%(title)s",
            "uploader:%(artist)s",
        ],

        "writethumbnail": True,        # needed so EmbedThumbnail has a source
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/favicon.ico")
def favicon():
    return "", 204


@app.route("/api/info", methods=["POST"])
def api_info():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"error": "No URLs provided"}), 400

    results = []
    with yt_dlp.YoutubeDL(_info_opts()) as ydl:
        for url in urls:
            try:
                info = ydl.extract_info(url, download=False)
                if "entries" in info:
                    entries = [
                        {
                            "title": e.get("title", "Unknown"),
                            "duration": e.get("duration"),
                            "thumbnail": e.get("thumbnail"),
                        }
                        for e in info["entries"] if e
                    ]
                    results.append({
                        "type": "playlist",
                        "url": url,
                        "playlist_title": info.get("title", "Untitled Playlist"),
                        "count": len(entries),
                        "entries": entries,
                    })
                else:
                    results.append({
                        "type": "video",
                        "url": url,
                        "title": info.get("title", "Unknown"),
                        "thumbnail": info.get("thumbnail"),
                        "duration": info.get("duration"),
                        "uploader": info.get("uploader"),
                    })
            except Exception as e:
                results.append({"type": "error", "url": url, "error": str(e)})

    return jsonify(results)


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def _run_download(task_id, urls):
    task = tasks[task_id]

    # --- Pass 1: count total tracks so the progress bar is meaningful ---
    task["status"] = "preparing"
    total = 0
    with yt_dlp.YoutubeDL(_info_opts()) as ydl:
        for url in urls:
            try:
                info = ydl.extract_info(url, download=False)
                if "entries" in info:
                    total += len([e for e in info["entries"] if e])
                else:
                    total += 1
            except Exception:
                total += 1   # assume 1 if info fails

    if total == 0:
        task["status"] = "error"
        task["error"] = "No downloadable items found."
        return

    task["total"] = total
    task["status"] = "downloading"

    completed = [0]

    def progress_hook(d):
        if d["status"] == "finished":
            completed[0] += 1
            task["completed"] = completed[0]
            task["progress"] = int(completed[0] / total * 100)
            # Strip the pre-conversion filename for display
            task["current_file"] = os.path.basename(d.get("filename", ""))

    try:
        with yt_dlp.YoutubeDL(_download_opts(progress_hook)) as ydl:
            ydl.download(urls)
        task["status"] = "finished"
        task["progress"] = 100
    except Exception as e:
        task["status"] = "error"
        task["error"] = str(e)


@app.route("/api/download", methods=["POST"])
def api_download():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body"}), 400
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"error": "No URLs provided"}), 400

    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "status": "queued",
        "progress": 0,
        "total": 0,
        "completed": 0,
        "current_file": "",
        "error": None,
    }

    thread = threading.Thread(target=_run_download, args=(task_id, urls), daemon=True)
    thread.start()

    return jsonify({"task_id": task_id})


@app.route("/api/status/<task_id>")
def api_status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


# ---------------------------------------------------------------------------
# File browser (serves already-downloaded MP3s)
# ---------------------------------------------------------------------------

@app.route("/api/files")
def api_files():
    """Return a flat list of all MP3s found recursively under DOWNLOAD_DIR."""
    mp3s = []
    for root, _, files in os.walk(DOWNLOAD_DIR):
        for f in files:
            if f.lower().endswith(".mp3"):
                rel = os.path.relpath(os.path.join(root, f), DOWNLOAD_DIR)
                mp3s.append(rel)
    mp3s.sort()
    return jsonify(mp3s)


@app.route("/api/files/<path:filename>")
def api_file_download(filename):
    """Download a specific file. Works with subdirectory paths."""
    full = os.path.realpath(os.path.join(DOWNLOAD_DIR, filename))
    # Security: prevent path traversal outside DOWNLOAD_DIR
    if not full.startswith(os.path.realpath(DOWNLOAD_DIR)):
        return jsonify({"error": "Forbidden"}), 403
    return send_file(full, as_attachment=True, download_name=os.path.basename(full))


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)