# Multi-runtime Dockerfile for Railway (Python 3.11 + Node.js 20 + ffmpeg)
FROM python:3.11-slim-bookworm

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install Node.js, npm, ffmpeg, and required system tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    ffmpeg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python backend dependencies
COPY agent/requirements.txt ./agent/requirements.txt
RUN pip install --no-cache-dir -r ./agent/requirements.txt

# Install Node.js frontend/proxy dependencies
COPY web/package.json ./web/package.json
RUN cd web && npm install --omit=dev

# Copy application code
COPY web/ ./web/
COPY agent/ ./agent/
COPY start.sh ./start.sh
RUN chmod +x ./start.sh

# Railway injects $PORT dynamically (fallback to 8085)
ENV PORT=8085
EXPOSE 8085

CMD ["./start.sh"]
