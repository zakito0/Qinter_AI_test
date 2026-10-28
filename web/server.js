const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8085;
const HTTPS_PORT = process.env.HTTPS_PORT || 8086;
const DIR = __dirname;
const keyPath = path.join(DIR, 'cert/key.pem');
const certPath = path.join(DIR, 'cert/cert.pem');
const hasHttpsCert = fs.existsSync(keyPath) && fs.existsSync(certPath);

// Determine local LAN IPv4 address (prioritizing physical Wi-Fi 192.168.x.x / 10.x.x.x)
function getLanIp() {
  const ifs = os.networkInterfaces();
  const candidates = [];
  for (const name of Object.keys(ifs)) {
    for (const net of ifs[name]) {
      if (net.family === 'IPv4' && !net.internal && !net.address.startsWith('127.')) {
        candidates.push(net.address);
      }
    }
  }
  // Prioritize typical home/office Wi-Fi subnets (192.168.* or 10.*) over virtual Hyper-V/WSL (172.*)
  const wifiIp = candidates.find(ip => ip.startsWith('192.168.')) ||
                 candidates.find(ip => ip.startsWith('10.')) ||
                 candidates[0] ||
                 'localhost';
  return wifiIp;
}

const LAN_IP = getLanIp();

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

const translationCache = new Map();
const ttsAudioCache = new Map();

async function getTtsAudioBuffer(text, lang) {
  const cacheKey = `${lang}_${text.trim().toLowerCase()}`;
  if (ttsAudioCache.has(cacheKey)) {
    return ttsAudioCache.get(cacheKey);
  }

  const query = encodeURIComponent(text.trim().slice(0, 250));
  const ttsUrl = `https://translate.google.com/translate_tts?ie=UTF-8&q=${query}&tl=${lang}&client=tw-ob`;
  const resp = await fetch(ttsUrl, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
    signal: AbortSignal.timeout(4500)
  });

  if (!resp.ok) {
    throw new Error(`TTS upstream error: ${resp.status}`);
  }

  const arrayBuf = await resp.arrayBuffer();
  const buffer = Buffer.from(arrayBuf);

  if (ttsAudioCache.size > 150) {
    const firstKey = ttsAudioCache.keys().next().value;
    ttsAudioCache.delete(firstKey);
  }
  ttsAudioCache.set(cacheKey, buffer);
  return buffer;
}

// Normalize language codes (e.g., 'zh' -> 'zh-CN')
function normalizeLangCode(code) {
  if (!code) return 'en';
  const c = code.toLowerCase().trim();
  if (c === 'zh' || c.startsWith('zh-')) return 'zh-CN';
  return c;
}

// Multi-Engine Robust Translation Service (Native Async, 100% Crash-Proof)
async function fetchTranslation(text, fromLang, toLang) {
  if (!text || text.trim() === '') return text;
  const sl = normalizeLangCode(fromLang);
  const tl = normalizeLangCode(toLang);
  if (sl === tl) return text;

  const cacheKey = `${sl}_${tl}_${text.trim().toLowerCase()}`;
  if (translationCache.has(cacheKey)) {
    return translationCache.get(cacheKey);
  }

  // Engine 1: Google GTX Single Endpoint (Ultra-fast direct neural translation)
  try {
    const query = encodeURIComponent(text.trim());
    const gtxUrl = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${sl}&tl=${tl}&dt=t&q=${query}`;
    const resp = await fetch(gtxUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      signal: AbortSignal.timeout(2200)
    });
    if (resp.ok) {
      const parsed = await resp.json();
      if (Array.isArray(parsed) && Array.isArray(parsed[0])) {
        const result = parsed[0].map(s => s[0]).filter(Boolean).join('');
        if (result && result.trim() !== '') {
          translationCache.set(cacheKey, result);
          return result;
        }
      }
    }
  } catch (ignored) {}

  // Engine 2: Google Dict-Chrome Gateway Fallback
  try {
    const query = encodeURIComponent(text.trim());
    const googleUrl = `https://clients5.google.com/translate_a/t?client=dict-chrome-ex&sl=${sl}&tl=${tl}&q=${query}`;
    const resp = await fetch(googleUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      signal: AbortSignal.timeout(2200)
    });
    if (resp.ok) {
      const parsed = await resp.json();
      let result = '';
      if (Array.isArray(parsed)) {
        result = parsed.join(' ');
      } else if (typeof parsed === 'string') {
        result = parsed;
      }
      if (result && result.trim() !== '') {
        translationCache.set(cacheKey, result);
        return result;
      }
    }
  } catch (ignored) {}

  // Engine 2 Fallback: MyMemory API
  try {
    const query = encodeURIComponent(text.trim());
    const myMemUrl = `https://api.mymemory.translated.net/get?q=${query}&langpair=${sl}|${tl}&de=qinterapp@gmail.com`;
    const resp = await fetch(myMemUrl, { signal: AbortSignal.timeout(3000) });
    if (resp.ok) {
      const parsed = await resp.json();
      if (parsed.responseData && parsed.responseData.translatedText) {
        let result = parsed.responseData.translatedText;
        if (!result.includes('MYMEMORY WARNING') && !result.includes('[ترجمة')) {
          translationCache.set(cacheKey, result);
          return result;
        }
      }
    }
  } catch (ignored) {}

  return text;
}

// Web Search Knowledge Tool (DuckDuckGo + Wikipedia REST API)
async function searchWebKnowledge(term) {
  const result = { summary: '', sources: [], title: term };
  try {
    const clean = term.replace(/[^a-zA-Z0-9 ]/g, ' ').trim().slice(0, 60);
    if (!clean) return result;

    // 1. DuckDuckGo Instant Answer API
    try {
      const ddgUrl = `https://api.duckduckgo.com/?q=${encodeURIComponent(clean)}&format=json&no_html=1`;
      const ddgResp = await fetch(ddgUrl, { signal: AbortSignal.timeout(2800) });
      if (ddgResp.ok) {
        const ddgData = await ddgResp.json();
        if (ddgData.AbstractText) {
          result.summary = ddgData.AbstractText;
          if (ddgData.Heading) result.title = ddgData.Heading;
          if (ddgData.AbstractURL) result.sources.push(ddgData.AbstractURL);
        }
      }
    } catch (e) {}

    // 2. Wikipedia REST API
    if (!result.summary) {
      try {
        const wikiUrl = `https://en.wikipedia.org/api/rest_v1/page/summary/${encodeURIComponent(clean.replace(/\s+/g, '_'))}`;
        const wikiResp = await fetch(wikiUrl, {
          headers: { 'User-Agent': 'QinterAI/1.0 (Web Search Tool)' },
          signal: AbortSignal.timeout(2800)
        });
        if (wikiResp.ok) {
          const wikiData = await wikiResp.json();
          if (wikiData.extract) {
            result.summary = wikiData.extract;
            result.title = wikiData.title || clean;
            if (wikiData.content_urls && wikiData.content_urls.desktop) {
              result.sources.push(wikiData.content_urls.desktop.page);
            }
          }
        }
      } catch (e) {}
    }
  } catch (e) {}
  return result;
}

async function handleAgentChat(body) {
  const {
    prompt = body.query || body.selectedPassage || 'Explain this transcript in detail.',
    transcript = '',
    history = [],
    mode = body.mode || 'AUTO_BALANCED',
    vps_host = body.vps_host || '127.0.0.1:11434',
    vps_model = body.vps_model || 'llama3.1:8b',
    nim_api_key = body.nim_api_key || '',
    nim_model = body.nim_model || 'nvidia/nemotron-3.5-bolt-e4b'
  } = body;

  const effectiveNimModel = (nim_model.includes('llama-3.2-11b') || nim_model.includes('mixtral'))
    ? 'nvidia/nemotron-3.5-bolt-e4b'
    : nim_model;

  // 1. Primary: Forward to Qinter LangGraph Agent Service (Port 8000)
  try {
    const agentResp = await fetch('http://127.0.0.1:8000/api/agent/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        prompt,
        transcript,
        history,
        search_web: Boolean(body.searchWeb || body.search_web),
        mode,
        vps_host,
        vps_model,
        nim_api_key,
        nim_model: effectiveNimModel
      }),
      signal: AbortSignal.timeout(45000)
    });
    if (agentResp.ok) {
      const data = await agentResp.json();
      if (data.reply) {
        return {
          reply: data.reply,
          model: `LangGraph (${mode === 'NVIDIA_NIM' ? nim_model : vps_model})`,
          citations: data.citations || [],
          webSources: data.webSources || [],
          webSearched: Boolean(data.webSearched),
          detectedLanguage: data.detectedLanguage || 'auto'
        };
      }
    }
  } catch (e) {
    console.log('[LangGraph Bridge] Note:', e.message);
  }

  // 2. Direct Ollama Fallback (if LangGraph server is paused)
  try {
    const docPrompt = transcript ? `\nAttached Document / Transcript Context:\n"""\n${transcript.slice(-4000)}\n"""\n` : '';
    const ollamaResp = await fetch('http://127.0.0.1:11434/api/generate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: vps_model || 'llama3.1:8b',
        prompt: `You are Qinter AI, an advanced, highly versatile multilingual AI assistant (like ChatGPT). Answer general questions, coding, explanations, and creative requests freely. If document context is provided below, reference it when asked; otherwise answer freely.\n${docPrompt}\nUser Prompt: ${prompt}\nAlways respond in the exact language of the user prompt with elegant markdown.`,
        stream: false,
        options: { temperature: 0.6 }
      }),
      signal: AbortSignal.timeout(20000)
    });
    if (ollamaResp.ok) {
      const oData = await ollamaResp.json();
      if (oData.response) {
        return {
          reply: oData.response.trim(),
          model: `Direct Local LLM (${vps_model || 'llama3.1:8b'})`,
          citations: []
        };
      }
    }
  } catch (e) {
    console.log('[Ollama Direct] Note:', e.message);
  }

  return {
    reply: `⚠️ **Agent Connection Notice**\nCould not reach the LangGraph Agent backend. Ensure the LangGraph service is running on your VPS: \`python agent/agent_service.py --host 0.0.0.0 --port 8000\`.`,
    model: 'Offline',
    citations: []
  };
}

// Gold SVG Favicon to avoid 404 errors
const FAVICON_SVG = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><circle cx="16" cy="16" r="14" fill="#0B0B0C" stroke="#D4AF37" stroke-width="2"/><circle cx="16" cy="16" r="6" fill="#D4AF37"/></svg>`;

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = parsedUrl.pathname;

  // Favicon handler
  if (pathname === '/favicon.ico') {
    res.writeHead(200, { 'Content-Type': 'image/svg+xml' });
    res.end(FAVICON_SVG);
    return;
  }

  // Info API: Returns LAN IP & Port for optical QR generation & secure context
  if (pathname === '/api/info') {
    const proto = req.headers['x-forwarded-proto'] || (hasHttpsCert ? 'https' : 'http');
    const host = req.headers.host || (LAN_IP + (PORT ? `:${PORT}` : ''));
    const origin = `${proto}://${host}`;
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({
      lanIp: LAN_IP,
      port: PORT,
      httpsPort: hasHttpsCert ? HTTPS_PORT : null,
      hasHttps: hasHttpsCert,
      publicOrigin: origin
    }));
    return;
  }

  // Mobile APK download disabled (Private development build - not distributed publicly)
  if (pathname === '/app-debug.apk' || pathname === '/download') {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not Found');
    return;
  }

  // Translation Proxy API
  if (pathname === '/api/translate') {
    const text = parsedUrl.searchParams.get('text') || '';
    const fromLang = parsedUrl.searchParams.get('from') || parsedUrl.searchParams.get('sl') || 'en';
    const toLang = parsedUrl.searchParams.get('to') || parsedUrl.searchParams.get('tl') || 'ar';

    fetchTranslation(text, fromLang, toLang)
      .then(translated => {
        if (!res.headersSent) {
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
          });
          res.end(JSON.stringify({ original: text, translated: translated || text }));
        }
      })
      .catch(() => {
        if (!res.headersSent) {
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
          });
          res.end(JSON.stringify({ original: text, translated: text }));
        }
      });
    return;
  }

  // Neural Audio TTS Stream API (Reliable audio speech for any language, 100% Apple iOS / Safari compatible)
  if (pathname === '/api/tts') {
    const text = (parsedUrl.searchParams.get('text') || '').trim();
    const lang = normalizeLangCode(parsedUrl.searchParams.get('lang') || 'en');

    if (!text) {
      if (!res.headersSent) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Missing text');
      }
      return;
    }

    getTtsAudioBuffer(text, lang)
      .then(buffer => {
        if (res.headersSent) return;
        const total = buffer.length;
        const range = req.headers.range;

        // Apple Safari requires HTTP 206 Partial Content with byte ranges for audio playback
        if (range) {
          const parts = range.replace(/bytes=/, "").split("-");
          const start = parseInt(parts[0], 10);
          const end = parts[1] ? parseInt(parts[1], 10) : total - 1;
          const chunk = buffer.slice(start, end + 1);

          res.writeHead(206, {
            'Content-Range': `bytes ${start}-${end}/${total}`,
            'Accept-Ranges': 'bytes',
            'Content-Length': chunk.length,
            'Content-Type': 'audio/mpeg',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=86400'
          });
          res.end(chunk);
        } else {
          res.writeHead(200, {
            'Content-Length': total,
            'Content-Type': 'audio/mpeg',
            'Accept-Ranges': 'bytes',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'public, max-age=86400'
          });
          res.end(buffer);
        }
      })
      .catch(err => {
        console.warn('TTS Generation error:', err.message);
        if (!res.headersSent) {
          res.writeHead(500, { 'Content-Type': 'text/plain' });
          res.end('TTS Error');
        }
      });
    return;
  }

  // Audio Recording Ingestion API (Receives treated audio slices)
  if (pathname === '/api/record' && req.method === 'POST') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      const buffer = Buffer.concat(chunks);
      console.log(`[Audio Server] Received treated audio slice: ${buffer.length} bytes`);
      res.writeHead(200, {
        'Content-Type': 'application/json; charset=utf-8',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(JSON.stringify({ success: true, bytes: buffer.length, receivedAt: Date.now() }));
    });
    return;
  }

  // AI Agent Docs Explainer & Details Adder API (with Web Search Intelligence)
  if (pathname === '/api/agent/chat') {
    const handleRequest = async (body) => {
      try {
        const responseData = await handleAgentChat(body);
        if (!res.headersSent) {
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
          });
          res.end(JSON.stringify(responseData));
        }
      } catch (err) {
        console.error('Agent chat error:', err);
        if (!res.headersSent) {
          res.writeHead(200, {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
          });
          res.end(JSON.stringify({
            reply: '### 💡 Speech Explanation\nThe speaker is communicating live interpreted speech. Connect to network to run in-depth web research.',
            webSearched: false,
            webSources: []
          }));
        }
      }
    };

    if (req.method === 'POST') {
      const chunks = [];
      req.on('data', chunk => chunks.push(chunk));
      req.on('end', () => {
        let body = {};
        try {
          body = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
        } catch (e) {}
        handleRequest(body);
      });
    } else {
      const body = {
        action: parsedUrl.searchParams.get('action') || 'EXPLAIN',
        query: parsedUrl.searchParams.get('query') || '',
        transcript: parsedUrl.searchParams.get('transcript') || '',
        selectedPassage: parsedUrl.searchParams.get('passage') || '',
        searchWeb: parsedUrl.searchParams.get('searchWeb') === 'true'
      };
      handleRequest(body);
    }
    return;
  }
  // Live Speech Transcription API Proxy -> Python ASR Service (Port 8000)
  // Ensures speech recognition works 100% across Opera GX, Chrome, Edge, Safari, Firefox, and Mobile
  if (pathname === '/api/transcribe') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', async () => {
      try {
        const bodyBuf = Buffer.concat(chunks);
        const lang = parsedUrl.searchParams.get('lang') || 'fr';
        const agentResp = await fetch(`http://127.0.0.1:8000/api/transcribe?lang=${encodeURIComponent(lang)}`, {
          method: 'POST',
          headers: {
            'Content-Type': req.headers['content-type'] || 'audio/webm'
          },
          body: bodyBuf,
          signal: AbortSignal.timeout(12000)
        });
        const respBuf = await agentResp.arrayBuffer();
        res.writeHead(agentResp.status, {
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(Buffer.from(respBuf));
      } catch (err) {
        console.warn('Transcription proxy error:', err.message);
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ text: '', error: err.message }));
      }
    });
    return;
  }

  // Auth API Proxy -> Python Agent Service (Port 8000)
  if (pathname.startsWith('/api/auth/')) {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', async () => {
      try {
        const bodyBuf = Buffer.concat(chunks);
        const headers = { 'Content-Type': req.headers['content-type'] || 'application/json' };
        if (req.headers['authorization']) headers['Authorization'] = req.headers['authorization'];
        const agentResp = await fetch(`http://127.0.0.1:8000${pathname}`, {
          method: req.method,
          headers,
          body: req.method !== 'GET' && req.method !== 'HEAD' ? bodyBuf : undefined,
          signal: AbortSignal.timeout(10000)
        });
        const respBuf = await agentResp.arrayBuffer();
        res.writeHead(agentResp.status, {
          'Content-Type': agentResp.headers.get('content-type') || 'application/json',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(Buffer.from(respBuf));
      } catch (err) {
        res.writeHead(502, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify({ error: 'Auth server unreachable. Make sure the agent backend is running.' }));
      }
    });
    return;
  }

  // Session Transcript Save API (saves markdown/text file)
  if (pathname === '/api/session/save' && req.method === 'POST') {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      try {
        const body = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
        const roomCode = (body.roomCode || 'ROOM').toUpperCase();
        const role = body.role || 'LISTENER';
        const content = body.content || '';
        const savedDir = path.join(DIR, 'saved_sessions');
        if (!fs.existsSync(savedDir)) fs.mkdirSync(savedDir, { recursive: true });

        const fileName = `Qinter_${roomCode}_${Date.now()}.md`;
        const filePath = path.join(savedDir, fileName);
        fs.writeFileSync(filePath, content, 'utf-8');

        res.writeHead(200, {
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*'
        });
        res.end(JSON.stringify({ success: true, fileName, filePath }));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Error saving session');
      }
    });
    return;
  }

  // Static File Serving
  let file = pathname;
  if (file === '/' || file === '') file = '/index.html';

  const fullPath = path.join(DIR, file);
  const ext = path.extname(fullPath).toLowerCase();

  fs.readFile(fullPath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Not Found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': 'no-cache'
    });
    res.end(data);
  });
});

// WebSocket Server for Multi-Device Real-Time Syncing (PC <-> Phone)
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomCode -> Set of WebSocket clients

function broadcastToRoom(roomCode, message, senderWs = null) {
  const clients = rooms.get(roomCode);
  if (!clients) return;
  const payload = JSON.stringify(message);
  for (const client of clients) {
    if (client !== senderWs && client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

wss.on('connection', (ws) => {
  let joinedRoom = null;

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'JOIN_ROOM') {
        const code = (msg.roomCode || '').toUpperCase();
        joinedRoom = code;
        if (!rooms.has(code)) {
          rooms.set(code, new Set());
        }
        rooms.get(code).add(ws);

        // Notify room of updated participant count
        const count = rooms.get(code).size;
        broadcastToRoom(code, { type: 'PARTICIPANTS_COUNT', count, roomCode: code });
        ws.send(JSON.stringify({ type: 'JOIN_OK', roomCode: code, count }));
      } else if (joinedRoom) {
        broadcastToRoom(joinedRoom, msg, ws);
      }
    } catch (e) {
      console.error('WS Message parsing error:', e);
    }
  });

  ws.on('close', () => {
    if (joinedRoom && rooms.has(joinedRoom)) {
      const set = rooms.get(joinedRoom);
      set.delete(ws);
      const count = set.size;
      if (count === 0) {
        rooms.delete(joinedRoom);
      } else {
        broadcastToRoom(joinedRoom, { type: 'PARTICIPANTS_COUNT', count, roomCode: joinedRoom });
      }
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Qinter AI Server running at:`);
  console.log(`  Local:   http://localhost:${PORT}`);
  console.log(`  Network: http://${LAN_IP}:${PORT}`);
});

if (hasHttpsCert) {
  try {
    const httpsOptions = {
      key: fs.readFileSync(keyPath),
      cert: fs.readFileSync(certPath)
    };
    const httpsServer = https.createServer(httpsOptions, (req, res) => {
      server.emit('request', req, res);
    });
    const wssHttps = new WebSocketServer({ server: httpsServer });
    wssHttps.on('connection', (ws) => {
      wss.emit('connection', ws);
    });
    httpsServer.listen(HTTPS_PORT, '0.0.0.0', () => {
      console.log(`  Secure HTTPS (Mic enabled for all devices):`);
      console.log(`    Local:   https://localhost:${HTTPS_PORT}`);
      console.log(`    Network: https://${LAN_IP}:${HTTPS_PORT}`);
    });
  } catch (err) {
    console.warn('[HTTPS Server] Startup notice:', err.message);
  }
}
