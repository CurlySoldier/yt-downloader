# Stage 1: fetch ffmpeg once — this layer is cached indefinitely
FROM python:3.11-slim AS ffmpeg-fetch
RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils \
    && rm -rf /var/lib/apt/lists/* \
    && curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz \
       | tar -xJ --strip-components=1 -C /usr/local/bin --wildcards '*/ffmpeg' '*/ffprobe' \
    && chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe

# Stage 2: app — copies binary from cache, never hits apt again
FROM python:3.11-slim
COPY --from=ffmpeg-fetch /usr/local/bin/ffmpeg /usr/local/bin/ffmpeg
COPY --from=ffmpeg-fetch /usr/local/bin/ffprobe /usr/local/bin/ffprobe

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY templates/ templates/

EXPOSE 5000
ENV DOWNLOAD_DIR=/downloads
ENV OUTPUT_TEMPLATE="%(playlist_title)s/%(playlist_index)s - %(title)s.%(ext)s"

VOLUME /downloads
CMD ["python", "app.py"]