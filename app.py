import os
import time
import uuid
import shutil
import threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify, send_from_directory, send_file, make_response
import yt_dlp
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError

app = Flask(__name__, static_folder="static")

# --- Config (override via environment variables) ---
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", os.path.expanduser("~/downloads"))
OUTPUT_TEMPLATE = os.environ.get(
    "OUTPUT_TEMPLATE",
    "%(playlist_title)s/%(playlist_index)s - %(title)s.%(ext)s",
)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
VIDEO_ID_ARCHIVE = os.environ.get(
    "VIDEO_ID_ARCHIVE",
    os.path.join(DOWNLOAD_DIR, ".video-id-archive.txt"),
)
VIDEO_ID_ARCHIVE_LOCK = threading.Lock()

# In-memory task store
tasks = {}
EVENT_HISTORY_LIMIT = 30


def _env_int(name, default):
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_csv(name, default_csv):
    value = os.environ.get(name, default_csv)
    return [v.strip() for v in value.split(",") if v.strip()]


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _push_event(task, message):
    events = task.setdefault("events", [])
    events.append({"at": _now_iso(), "message": message})
    if len(events) > EVENT_HISTORY_LIMIT:
        del events[:-EVENT_HISTORY_LIMIT]


def _set_phase(task, phase):
    task["phase"] = phase
    task["status"] = phase


def _classify_error(message):
    text = (message or "").lower()
    if "403" in text or "forbidden" in text:
        return "http_403"
    if any(x in text for x in ("timed out", "timeout", "connection", "network")):
        return "network"
    if any(x in text for x in ("extractor", "unable to extract", "unsupported url")):
        return "extractor"
    return "unknown"


def _error_hint(error_type):
    if error_type == "http_403":
        return "Source denied access (HTTP 403). Try cookies or retry later."
    if error_type == "network":
        return "Network issue detected. Retry may succeed."
    if error_type == "extractor":
        return "Extractor could not parse this source. Update yt-dlp or try another URL."
    return "Unexpected downloader error. Check logs for details."


def _read_archived_video_ids():
    if not os.path.exists(VIDEO_ID_ARCHIVE):
        return set()

    archived = set()
    with VIDEO_ID_ARCHIVE_LOCK:
        try:
            with open(VIDEO_ID_ARCHIVE, "r", encoding="utf-8") as fh:
                for line in fh:
                    value = line.strip()
                    if value:
                        archived.add(value)
        except OSError:
            return set()
    return archived


def _append_archived_video_id(video_id):
    if not video_id:
        return

    archive_dir = os.path.dirname(VIDEO_ID_ARCHIVE)
    if archive_dir:
        os.makedirs(archive_dir, exist_ok=True)

    with VIDEO_ID_ARCHIVE_LOCK:
        with open(VIDEO_ID_ARCHIVE, "a", encoding="utf-8") as fh:
            fh.write(f"{video_id}\n")


def _sanitize_rel_path(value):
    text = (value or "").replace("\\", "/").strip()
    text = text.lstrip("/")
    text = os.path.normpath(text)
    if text in ("", "."):
        return ""
    if text.startswith("../") or text == "..":
        raise ValueError("Path escapes download directory")
    return text


def _resolve_library_path(rel_path="", allow_missing=False):
    safe_rel = _sanitize_rel_path(rel_path)
    candidate = os.path.realpath(os.path.join(DOWNLOAD_DIR, safe_rel))
    base = os.path.realpath(DOWNLOAD_DIR)
    if candidate != base and not candidate.startswith(base + os.sep):
        raise ValueError("Path escapes download directory")
    if not allow_missing and not os.path.exists(candidate):
        raise FileNotFoundError("Path not found")
    return candidate


def _relative_library_path(full_path):
    base = os.path.realpath(DOWNLOAD_DIR)
    resolved = os.path.realpath(full_path)
    if resolved == base:
        return ""
    return os.path.relpath(resolved, base).replace("\\", "/")


def _validate_name(name):
    value = (name or "").strip()
    if not value:
        raise ValueError("Name is required")
    if any(ch in value for ch in ("/", "\\")):
        raise ValueError("Name cannot contain path separators")
    if value in (".", ".."):
        raise ValueError("Invalid name")
    return value


def _read_song_metadata(full_path):
    try:
        tags = EasyID3(full_path)
    except ID3NoHeaderError:
        return {
            "title": "",
            "artist": "",
            "album": "",
            "track": "",
            "year": "",
            "genre": "",
        }

    def first(key):
        values = tags.get(key, [])
        return values[0] if values else ""

    return {
        "title": first("title"),
        "artist": first("artist"),
        "album": first("album"),
        "track": first("tracknumber"),
        "year": first("date"),
        "genre": first("genre"),
    }


def _write_song_metadata(full_path, metadata):
    try:
        tags = EasyID3(full_path)
    except ID3NoHeaderError:
        tags = EasyID3()

    fields = {
        "title": "title",
        "artist": "artist",
        "album": "album",
        "track": "tracknumber",
        "year": "date",
        "genre": "genre",
    }
    for src, key in fields.items():
        value = (metadata.get(src) or "").strip()
        if value:
            tags[key] = [value]
        elif key in tags:
            del tags[key]

    tags.save(full_path)


def _entry_to_url(entry, parent_url):
    raw = entry.get("webpage_url") or entry.get("url")
    if raw and isinstance(raw, str) and raw.startswith("http"):
        return raw

    video_id = entry.get("id") or raw
    if isinstance(video_id, str) and video_id:
        ie_key = (entry.get("ie_key") or "").lower()
        if "youtube" in ie_key or "youtube.com" in parent_url or "youtu.be" in parent_url:
            return f"https://www.youtube.com/watch?v={video_id}"

    return parent_url


def _entry_video_id(entry):
    video_id = entry.get("id")
    if isinstance(video_id, str):
        video_id = video_id.strip()
    return video_id or None


def _build_download_items(urls, task):
    items = []
    with yt_dlp.YoutubeDL(_info_opts()) as ydl:
        for url in urls:
            try:
                info = ydl.extract_info(url, download=False)
            except Exception as e:
                _push_event(task, f"Metadata lookup failed for {url}: {e}")
                items.append({"url": url, "title": url, "source": url})
                continue

            if info and "entries" in info:
                playlist_title = info.get("title") or "Playlist"
                entries = [e for e in (info.get("entries") or []) if e]
                if not entries:
                    items.append({"url": url, "title": playlist_title, "source": url})
                    continue

                for idx, entry in enumerate(entries, start=1):
                    items.append(
                        {
                            "url": _entry_to_url(entry, url),
                            "title": entry.get("title") or f"{playlist_title} #{idx}",
                            "source": url,
                            "video_id": _entry_video_id(entry),
                        }
                    )
            else:
                items.append(
                    {
                        "url": url,
                        "title": (info or {}).get("title") or url,
                        "source": url,
                        "video_id": _entry_video_id(info or {}),
                    }
                )
    return items


def _download_strategies():
    players = _env_csv("YTDLP_PLAYER_CLIENTS", "web,android")
    cookie_file = os.environ.get("YTDLP_COOKIEFILE")

    strategies = [
        {"name": "default", "extra": {}},
        {
            "name": "alt_client",
            "extra": {"extractor_args": {"youtube": {"player_client": players}}},
        },
    ]

    if cookie_file and os.path.exists(cookie_file):
        strategies.append({"name": "cookie", "extra": {"cookiefile": cookie_file}})

    return strategies


# ---------------------------------------------------------------------------
# yt-dlp option factories
# ---------------------------------------------------------------------------

def _resilience_opts():
    """Enable YouTube JS support and retry transient extraction/download errors."""
    return {
        "js_runtimes": {"node": {}},
        "retries": _env_int("YTDLP_RETRIES", 10),
        "fragment_retries": _env_int("YTDLP_FRAGMENT_RETRIES", 10),
        "extractor_retries": _env_int("YTDLP_EXTRACTOR_RETRIES", 3),
        "file_access_retries": _env_int("YTDLP_FILE_ACCESS_RETRIES", 3),
        "socket_timeout": _env_int("YTDLP_SOCKET_TIMEOUT", 30),
    }


def _info_opts():
    """Lightweight opts for metadata-only fetches."""
    return {
        **_resilience_opts(),
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
    }


def _download_opts(progress_hook, extra_opts=None):
    """Full download opts matching the original CLI flags."""
    opts = {
        **_resilience_opts(),
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
    if extra_opts:
        opts.update(extra_opts)
    return opts


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    resp = make_response(send_from_directory("templates", "index.html"))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


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

    task["started_at"] = _now_iso()
    _set_phase(task, "preparing")
    task["summary"] = "Preparing download list..."
    _push_event(task, "Task queued")

    items = _build_download_items(urls, task)
    total = len(items)
    if total == 0:
        task["phase"] = "error"
        task["status"] = "error"
        task["error"] = "No downloadable items found."
        task["last_error"] = task["error"]
        task["summary"] = task["error"]
        task["finished_at"] = _now_iso()
        _push_event(task, task["error"])
        return

    task["total"] = total
    task["summary"] = f"Ready to download {total} item{'s' if total != 1 else ''}."
    _push_event(task, task["summary"])

    strategies = _download_strategies()
    archived_video_ids = _read_archived_video_ids()

    def progress_hook(d):
        status = d.get("status")
        filename = d.get("filename")
        if filename:
            task["current_file"] = os.path.basename(filename)

        if status == "downloading":
            downloaded = d.get("downloaded_bytes")
            total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
            if downloaded is not None and total_bytes:
                task["current_item_pct"] = max(0, min(100, int(downloaded * 100 / total_bytes)))
            speed = d.get("speed")
            task["current_speed"] = int(speed) if speed else None
            eta = d.get("eta")
            task["eta_seconds"] = int(eta) if eta is not None else None
        elif status == "finished":
            task["current_item_pct"] = 100

    try:
        for item in items:
            item_video_id = item.get("video_id")
            if item_video_id and item_video_id in archived_video_ids:
                task["completed"] += 1
                task["skipped"] += 1
                task["progress"] = int(task["completed"] / total * 100)
                _push_event(task, f"Skipped duplicate '{item['title']}' (video id: {item_video_id})")
                continue

            task["current_item"] = item["title"]
            task["current_item_pct"] = 0
            task["current_speed"] = None
            task["eta_seconds"] = None

            succeeded = False
            last_error = ""
            last_error_type = "unknown"

            for idx, strategy in enumerate(strategies):
                if idx == 0:
                    _set_phase(task, "downloading")
                    task["summary"] = f"Downloading {item['title']}"
                else:
                    _set_phase(task, "retrying")
                    task["summary"] = f"Retrying {item['title']} ({strategy['name']})"
                    _push_event(task, f"Retrying '{item['title']}' via {strategy['name']}")

                try:
                    with yt_dlp.YoutubeDL(_download_opts(progress_hook, strategy["extra"])) as ydl:
                        code = ydl.download([item["url"]])
                    if code == 0:
                        succeeded = True
                        _push_event(task, f"Downloaded '{item['title']}' via {strategy['name']}")
                        break
                    last_error = f"yt-dlp exited with code {code}"
                    last_error_type = _classify_error(last_error)
                except Exception as e:
                    last_error = str(e)
                    last_error_type = _classify_error(last_error)

                if idx < len(strategies) - 1:
                    time.sleep(1)

            task["completed"] += 1
            task["progress"] = int(task["completed"] / total * 100)

            if succeeded:
                task["succeeded"] += 1
                if item_video_id and item_video_id not in archived_video_ids:
                    _append_archived_video_id(item_video_id)
                    archived_video_ids.add(item_video_id)
            else:
                task["failed"] += 1
                hint = _error_hint(last_error_type)
                issue_message = f"{last_error} ({hint})" if last_error else hint
                task["last_error"] = issue_message
                task["error"] = issue_message
                task["errors"].append(
                    {
                        "url": item["url"],
                        "source": item["source"],
                        "title": item["title"],
                        "error_type": last_error_type,
                        "message": issue_message,
                    }
                )
                _push_event(task, f"Failed '{item['title']}': {issue_message}")

        task["current_item"] = ""
        task["current_item_pct"] = 0
        task["current_speed"] = None
        task["eta_seconds"] = None

        if task["failed"]:
            task["phase"] = "finished_with_errors"
            task["status"] = "finished_with_errors"
            skipped_clause = ""
            if task["skipped"]:
                skipped_clause = (
                    f" and {task['skipped']} duplicate"
                    f"{'s' if task['skipped'] != 1 else ''} skipped"
                )
            task["summary"] = (
                f"Completed with {task['failed']} failure"
                f"{'s' if task['failed'] != 1 else ''}"
                f"{skipped_clause}."
            )
        elif task["skipped"]:
            task["phase"] = "finished"
            task["status"] = "finished"
            task["summary"] = (
                f"Completed successfully with {task['skipped']} duplicate"
                f"{'s' if task['skipped'] != 1 else ''} skipped."
            )
        else:
            task["phase"] = "finished"
            task["status"] = "finished"
            task["summary"] = "All downloads finished successfully."

        task["progress"] = 100
        task["finished_at"] = _now_iso()
        _push_event(task, task["summary"])
    except Exception as e:
        task["phase"] = "error"
        task["status"] = "error"
        task["error"] = str(e)
        task["last_error"] = str(e)
        task["summary"] = f"Download task failed: {e}"
        task["finished_at"] = _now_iso()
        _push_event(task, task["summary"])


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
        "task_id": task_id,
        "status": "queued",
        "phase": "queued",
        "summary": "Waiting to start...",
        "progress": 0,
        "total": 0,
        "completed": 0,
        "succeeded": 0,
        "skipped": 0,
        "failed": 0,
        "current_file": "",
        "current_item": "",
        "current_item_pct": 0,
        "current_speed": None,
        "eta_seconds": None,
        "error": None,
        "last_error": None,
        "errors": [],
        "events": [],
        "started_at": None,
        "finished_at": None,
    }

    thread = threading.Thread(target=_run_download, args=(task_id, urls), daemon=True)
    thread.start()

    return jsonify({"task_id": task_id})


@app.route("/api/status/<task_id>")
def api_status(task_id):
    task = tasks.get(task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    payload = dict(task)
    payload["task_id"] = task_id
    return jsonify(payload)


@app.route("/api/tasks")
def api_tasks():
    active_only = request.args.get("active") == "1"
    out = []
    active_states = {"queued", "preparing", "downloading", "retrying", "pending"}

    for task_id, task in tasks.items():
        status = task.get("status")
        if active_only and status not in active_states:
            continue

        out.append(
            {
                "task_id": task_id,
                "status": status,
                "phase": task.get("phase"),
                "summary": task.get("summary"),
                "progress": task.get("progress", 0),
                "total": task.get("total", 0),
                "completed": task.get("completed", 0),
                "succeeded": task.get("succeeded", 0),
                "skipped": task.get("skipped", 0),
                "failed": task.get("failed", 0),
                "started_at": task.get("started_at"),
                "finished_at": task.get("finished_at"),
            }
        )

    out.sort(key=lambda t: t.get("started_at") or "", reverse=True)
    return jsonify(out)


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


@app.route("/api/library")
def api_library():
    rel_path = request.args.get("path", "")
    try:
        full = _resolve_library_path(rel_path)
        if not os.path.isdir(full):
            return jsonify({"error": "Not a folder"}), 400

        folders = []
        files = []
        for name in sorted(os.listdir(full), key=str.casefold):
            child_full = os.path.join(full, name)
            rel = _relative_library_path(child_full)
            if os.path.isdir(child_full):
                folders.append({"name": name, "path": rel})
            elif name.lower().endswith(".mp3"):
                files.append({"name": name, "path": rel})

        return jsonify(
            {
                "path": _relative_library_path(full),
                "folders": folders,
                "files": files,
            }
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except FileNotFoundError:
        return jsonify({"error": "Folder not found"}), 404


@app.route("/api/library/song")
def api_library_song_get():
    rel_path = request.args.get("path", "")
    try:
        full = _resolve_library_path(rel_path)
        if not os.path.isfile(full) or not full.lower().endswith(".mp3"):
            return jsonify({"error": "Song not found"}), 404
        stat = os.stat(full)
        return jsonify(
            {
                "path": _relative_library_path(full),
                "name": os.path.basename(full),
                "metadata": _read_song_metadata(full),
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(timespec="seconds"),
            }
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except FileNotFoundError:
        return jsonify({"error": "Song not found"}), 404


@app.route("/api/library/song", methods=["PATCH"])
def api_library_song_patch():
    data = request.get_json() or {}
    rel_path = data.get("path", "")
    metadata = data.get("metadata") or {}
    try:
        full = _resolve_library_path(rel_path)
        if not os.path.isfile(full) or not full.lower().endswith(".mp3"):
            return jsonify({"error": "Song not found"}), 404
        _write_song_metadata(full, metadata)
        return jsonify(
            {
                "ok": True,
                "path": _relative_library_path(full),
                "metadata": _read_song_metadata(full),
            }
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except FileNotFoundError:
        return jsonify({"error": "Song not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/library/folder", methods=["POST"])
def api_library_folder_create():
    data = request.get_json() or {}
    parent_path = data.get("parent_path", "")
    name = data.get("name", "")
    try:
        safe_name = _validate_name(name)
        parent_full = _resolve_library_path(parent_path)
        if not os.path.isdir(parent_full):
            return jsonify({"error": "Parent must be a folder"}), 400
        new_full = _resolve_library_path(
            os.path.join(_relative_library_path(parent_full), safe_name),
            allow_missing=True,
        )
        os.makedirs(new_full, exist_ok=False)
        return jsonify({"ok": True, "path": _relative_library_path(new_full)})
    except FileExistsError:
        return jsonify({"error": "Folder already exists"}), 409
    except (ValueError, FileNotFoundError) as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/library/rename", methods=["POST"])
def api_library_rename():
    data = request.get_json() or {}
    source_path = data.get("path", "")
    new_name = data.get("new_name", "")
    try:
        safe_name = _validate_name(new_name)
        source_full = _resolve_library_path(source_path)
        if source_full == os.path.realpath(DOWNLOAD_DIR):
            return jsonify({"error": "Cannot rename root folder"}), 400
        parent_full = os.path.dirname(source_full)
        target_full = _resolve_library_path(
            os.path.join(_relative_library_path(parent_full), safe_name),
            allow_missing=True,
        )
        if os.path.exists(target_full):
            return jsonify({"error": "Target already exists"}), 409
        os.rename(source_full, target_full)
        return jsonify(
            {
                "ok": True,
                "path": _relative_library_path(target_full),
            }
        )
    except FileNotFoundError:
        return jsonify({"error": "Path not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/library/move", methods=["POST"])
def api_library_move():
    data = request.get_json() or {}
    source_path = data.get("source_path", "")
    target_dir = data.get("target_dir", "")
    try:
        source_full = _resolve_library_path(source_path)
        if source_full == os.path.realpath(DOWNLOAD_DIR):
            return jsonify({"error": "Cannot move root folder"}), 400
        target_full = _resolve_library_path(target_dir)
        if not os.path.isdir(target_full):
            return jsonify({"error": "Target is not a folder"}), 400

        final_full = _resolve_library_path(
            os.path.join(_relative_library_path(target_full), os.path.basename(source_full)),
            allow_missing=True,
        )
        if os.path.exists(final_full):
            return jsonify({"error": "Target already has this name"}), 409

        shutil.move(source_full, final_full)
        return jsonify({"ok": True, "path": _relative_library_path(final_full)})
    except FileNotFoundError:
        return jsonify({"error": "Path not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/library/path", methods=["DELETE"])
def api_library_delete():
    data = request.get_json(silent=True) or {}
    rel_path = data.get("path") or request.args.get("path", "")
    try:
        full = _resolve_library_path(rel_path)
        if full == os.path.realpath(DOWNLOAD_DIR):
            return jsonify({"error": "Cannot delete root folder"}), 400

        if os.path.isdir(full):
            shutil.rmtree(full)
        else:
            os.remove(full)
        return jsonify({"ok": True})
    except FileNotFoundError:
        return jsonify({"error": "Path not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/library/download")
def api_library_download():
    rel_path = request.args.get("path", "")
    try:
        full = _resolve_library_path(rel_path)
        if not os.path.isfile(full):
            return jsonify({"error": "File not found"}), 404
        return send_file(full, as_attachment=True, download_name=os.path.basename(full))
    except FileNotFoundError:
        return jsonify({"error": "File not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


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
