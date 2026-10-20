# Node.js runtime for yt-dlp's YouTube JavaScript challenges
FROM node:22-bookworm-slim AS node-runtime

# App
FROM python:3.11-slim
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY --from=node-runtime /usr/local/bin/node /usr/local/bin/node

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY templates/ templates/

EXPOSE 5000
ENV DOWNLOAD_DIR=/downloads
ENV OUTPUT_TEMPLATE="%(playlist_title)s/%(playlist_index)s - %(title)s.%(ext)s"

VOLUME /downloads
CMD ["gunicorn", "-k", "gthread", "--threads", "8", "--workers", "1", "--timeout", "120", "--bind", "0.0.0.0:5000", "app:app"]
