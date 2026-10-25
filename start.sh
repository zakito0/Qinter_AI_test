#!/bin/bash
set -e

echo "=========================================================="
echo " Starting Qinter Web Lite on Railway"
echo " Node Server: 0.0.0.0:${PORT:-8085}"
echo " Python ASR:  127.0.0.1:8000"
echo "=========================================================="

# 1. Start Python ASR & Agent Service in background on port 8000
python3 agent/agent_service.py --host 127.0.0.1 --port 8000 &
PYTHON_PID=$!

# 2. Give Python service a moment to bind
for i in {1..20}; do
  if curl -s http://127.0.0.1:8000/health > /dev/null 2>&1; then
    echo "Python ASR & Agent service is online."
    break
  fi
  sleep 0.3
done

# 3. Start Node.js web server as foreground process
exec node web/server.js
