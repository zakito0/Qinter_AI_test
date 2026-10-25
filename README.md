# Qinter AI - Web Speech Edition (Railway Test Deployment)

This repository contains the standalone **Web Lite** edition of Qinter, pre-configured for automated **1-click cloud deployment on Railway**.

---

## 🌟 Architecture Overview

```
                      ┌─────────────────────────────────────────┐
                      │            Railway Edge Proxy           │
                      │  (Automatic SSL / HTTPS & WSS Support)  │
                      └────────────────────┬────────────────────┘
                                           │
                                     Port $PORT
                                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Docker Container                                                            │
│                                                                             │
│   ┌───────────────────────────────┐     Proxy     ┌──────────────────────┐  │
│   │ Node.js Server (web/server.js)│ ────────────> │ Python ASR & Agent   │  │
│   │ • WebSockets (Real-time sync) │ /api/transcribe│ (agent_service.py)   │  │
│   │ • Static HTML5 / Audio DSP    │               │ • SpeechRecognition  │  │
│   │ • Neural Google TTS & Trans   │               │ • ffmpeg mono 16kHz  │  │
│   └───────────────────────────────┘               └──────────────────────┘  │
│             Port $PORT                                   Port 8000          │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 How to Deploy on Railway

1. **Create / Open Railway Project**:
   - Go to [railway.app](https://railway.app) and sign in.
   - Click **New Project** ➔ **Deploy from GitHub repo**.
   - Select `zakito0/Qinter_AI_test`.

2. **Automatic Detection**:
   - Railway will automatically detect `Dockerfile` and `railway.json`.
   - It will install Python, Node.js 20, and `ffmpeg`.
   - Both services start automatically via `start.sh`.

3. **Generate Public Domain**:
   - In Railway dashboard, go to **Settings** ➔ **Networking** ➔ **Generate Domain**.
   - Railway will assign a secure HTTPS domain (e.g. `https://qinter-ai-test.up.railway.app`).
   - Open that URL on your phone or laptop.
   - The microphone and live real-time speech translation room work out of the box with zero configuration!

---

## 🔒 Privacy & Security
- **No APK Included**: This repository strictly contains the browser speech room. No Android binaries or private credentials are included.
- **Microphone Enabled**: Served over Railway's valid HTTPS edge certificate, enabling microphone hardware permissions on iOS Safari and Android Chrome.
