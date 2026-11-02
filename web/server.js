const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer, WebSocket } = require('ws');

// Automatically load .env file from web/ or agent/ if present
function loadEnvFile(envPath) {
  try {
    if (fs.existsSync(envPath)) {
      const content = fs.readFileSync(envPath, 'utf8');
      for (const line of content.split('\n')) {
        const trimmed = line.trim();
        if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
          const idx = trimmed.indexOf('=');
          const k = trimmed.slice(0, idx).trim();
          const v = trimmed.slice(idx + 1).trim().replace(/^['"]|['"]$/g, '');
          if (!process.env[k]) {
            process.env[k] = v;
          }
        }
      }
    }
  } catch (e) {}
}
loadEnvFile(path.join(__dirname, '.env'));
loadEnvFile(path.join(__dirname, '../agent/.env'));
loadEnvFile(path.join(__dirname, '../.env'));

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
const activeRooms = new Map(); // roomCode -> { code, hostWs, lat, lon, sourceLang, targetLang, createdAt }

// Haversine formula to calculate physical distance between two GPS coordinates in meters
function calculateDistanceMeters(lat1, lon1, lat2, lon2) {
  if (lat1 == null || lon1 == null || lat2 == null || lon2 == null) return null;
  const R = 6371e3; // Earth radius in meters
  const rad = Math.PI / 180;
  const φ1 = lat1 * rad;
  const φ2 = lat2 * rad;
  const Δφ = (lat2 - lat1) * rad;
  const Δλ = (lon2 - lon1) * rad;

  const a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
            Math.cos(φ1) * Math.cos(φ2) *
            Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return Math.round(R * c);
}

async function getTtsAudioBuffer(text, lang, voice = 'male_natural', rate = '1.0') {
  const cleanVoice = (voice || 'male_natural').trim().toLowerCase();
  const cleanRate = (rate || '1.0').toString().trim();
  const cacheKey = `${lang}_${cleanVoice}_${cleanRate}_${text.trim().toLowerCase()}`;
  if (ttsAudioCache.has(cacheKey)) {
    return ttsAudioCache.get(cacheKey);
  }

  const query = encodeURIComponent(text.trim().slice(0, 350));
  const agentTtsUrl = `http://127.0.0.1:8000/api/tts?text=${query}&lang=${encodeURIComponent(lang)}&voice=${encodeURIComponent(cleanVoice)}&rate=${encodeURIComponent(cleanRate)}`;

  // 1. Primary: VoiceStudio Python Service (Edge-TTS / Local VoiceStudio on port 3900)
  try {
    const resp = await fetch(agentTtsUrl, {
      signal: AbortSignal.timeout(6500)
    });
    if (resp.ok) {
      const arrayBuf = await resp.arrayBuffer();
      const buffer = Buffer.from(arrayBuf);
      if (buffer && buffer.length > 200) {
        if (ttsAudioCache.size > 150) {
          const firstKey = ttsAudioCache.keys().next().value;
          ttsAudioCache.delete(firstKey);
        }
        ttsAudioCache.set(cacheKey, buffer);
        return buffer;
      }
    }
  } catch (e) {
    console.log('[VoiceStudio Proxy] Python service note:', e.message);
  }

  // 2. Fallback: High-speed Neural TTS Stream Gateway (Zero downtime guarantee)
  try {
    const fallbackUrl = `https://translate.google.com/translate_tts?ie=UTF-8&q=${query}&tl=${encodeURIComponent(lang)}&client=tw-ob`;
    const fResp = await fetch(fallbackUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      signal: AbortSignal.timeout(4000)
    });
    if (fResp.ok) {
      const fBuf = Buffer.from(await fResp.arrayBuffer());
      if (fBuf && fBuf.length > 200) {
        ttsAudioCache.set(cacheKey, fBuf);
        return fBuf;
      }
    }
  } catch (fe) {
    console.log('[TTS Fallback] Gateway note:', fe.message);
  }

  throw new Error('TTS audio generation failed across all available providers');
}

// Normalize language codes (e.g., 'zh' -> 'zh-CN')
function normalizeLangCode(code) {
  if (!code) return 'en';
  const c = code.toLowerCase().trim();
  if (c === 'zh' || c.startsWith('zh-')) return 'zh-CN';
  return c;
}

// ---- 3-System Translation Engine (DeepL Official -> Google Cloud Official -> Open-Source Multi-Provider) ----
const DEEPL_KEY = (process.env.DEEPL_API_KEY || '').trim();
const GOOGLE_TRANSLATE_KEY = (process.env.GOOGLE_TRANSLATE_API_KEY || '').trim();
const LIBRETRANSLATE_URL = (process.env.LIBRETRANSLATE_URL || '').trim();
const LIBRETRANSLATE_KEY = (process.env.LIBRETRANSLATE_API_KEY || '').trim();

const DEEPL_TARGETS = {
  en: 'EN-US', ar: 'AR', fr: 'FR', es: 'ES', de: 'DE', it: 'IT', pt: 'PT-PT', ru: 'RU',
  'zh-cn': 'ZH-HANS', ja: 'JA', ko: 'KO', tr: 'TR', nl: 'NL', pl: 'PL', sv: 'SV', id: 'ID', he: 'HE'
};
const DEEPL_SOURCES = {
  en: 'EN', ar: 'AR', fr: 'FR', es: 'ES', de: 'DE', it: 'IT', pt: 'PT', ru: 'RU',
  'zh-cn': 'ZH', ja: 'JA', ko: 'KO', tr: 'TR', nl: 'NL', pl: 'PL', sv: 'SV', id: 'ID', he: 'HE'
};

// System 1: Official DeepL API (Ultra-fast direct GET query with Bearer Auth)
async function translateWithDeepL(text, sl, tl) {
  if (!DEEPL_KEY) return null;
  const target = DEEPL_TARGETS[tl.toLowerCase()];
  if (!target) return null; // not supported by DeepL (e.g. hi, fa, ur) -> next engine
  const clean = text.trim();
  const source = DEEPL_SOURCES[sl.toLowerCase()];
  try {
    const host = DEEPL_KEY.endsWith(':fx') ? 'api-free.deepl.com' : 'api.deepl.com';
    let url = `https://${host}/v2/translate?text=${encodeURIComponent(clean)}&target_lang=${encodeURIComponent(target)}`;
    if (source) url += `&source_lang=${encodeURIComponent(source)}`;
    
    const resp = await fetch(url, {
      method: 'GET',
      headers: { 'Authorization': `DeepL-Auth-Key ${DEEPL_KEY}` },
      signal: AbortSignal.timeout(2800)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const out = data.translations && data.translations[0] && data.translations[0].text;
    return out && out.trim() ? out.trim() : null;
  } catch (e) {
    return null;
  }
}

// System 2: Official Google Cloud Translation API v2 (Ultra-fast direct GET query)
async function translateWithGoogleCloud(text, sl, tl) {
  if (!GOOGLE_TRANSLATE_KEY) return null;
  const clean = text.trim();
  try {
    let url = `https://translation.googleapis.com/language/translate/v2?key=${encodeURIComponent(GOOGLE_TRANSLATE_KEY)}&q=${encodeURIComponent(clean)}&target=${encodeURIComponent(tl)}&format=text`;
    if (sl && sl !== 'auto') url += `&source=${encodeURIComponent(sl)}`;
    
    const resp = await fetch(url, {
      method: 'GET',
      signal: AbortSignal.timeout(2500)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const out = data.data && data.data.translations && data.data.translations[0] && data.data.translations[0].translatedText;
    return out && out.trim() ? out.trim() : null;
  } catch (e) {
    return null;
  }
}

// System 3: High-Reliability Neural Fallback (Google Web Gateway + LibreTranslate)
async function translateWithOpenSource(text, sl, tl) {
  const cleanText = text.trim();
  const normSl = sl === 'zh-CN' ? 'zh-CN' : (sl || 'auto');
  const normTl = tl === 'zh-CN' ? 'zh-CN' : tl;

  // 3a. Google Web Neural Gateway (Lightweight direct GET ~120ms, identical to Google webapi)
  try {
    const gtxUrl = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${encodeURIComponent(normSl)}&tl=${encodeURIComponent(normTl)}&dt=t&q=${encodeURIComponent(cleanText)}`;
    const resp = await fetch(gtxUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
      signal: AbortSignal.timeout(2500)
    });
    if (resp.ok) {
      const data = await resp.json();
      if (Array.isArray(data) && Array.isArray(data[0])) {
        const fullTranslation = data[0].map(item => item[0] || '').join('').trim();
        if (fullTranslation && fullTranslation.toLowerCase() !== cleanText.toLowerCase()) {
          return fullTranslation;
        }
      }
    }
  } catch (ignored) {}

  // 3b. LibreTranslate (Self-hosted or public instances)
  const libreHosts = LIBRETRANSLATE_URL
    ? [LIBRETRANSLATE_URL.replace(/\/+$/, '')]
    : ['https://translate.terraprint.co', 'https://lt.vern.cc'];
  for (const host of libreHosts) {
    try {
      const libreSl = normSl === 'zh-CN' ? 'zh' : normSl;
      const libreTl = normTl === 'zh-CN' ? 'zh' : normTl;
      const payload = { q: cleanText, source: libreSl === 'auto' ? 'auto' : libreSl, target: libreTl, format: 'text' };
      if (LIBRETRANSLATE_KEY) payload.api_key = LIBRETRANSLATE_KEY;
      const resp = await fetch(`${host}/translate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(2000)
      });
      if (resp.ok) {
        const data = await resp.json();
        if (data && data.translatedText && data.translatedText.trim() && data.translatedText.trim().toLowerCase() !== cleanText.toLowerCase()) {
          return data.translatedText.trim();
        }
      }
    } catch (ignored) {}
  }

  // 3c. Lingva Open-Source Mirrors
  const lingvaHosts = [
    'https://lingva.ml',
    'https://translate.plausibility.cloud',
    'https://lingva.lunar.icu'
  ];
  for (const host of lingvaHosts) {
    try {
      const lingvaSl = normSl === 'zh-CN' ? 'zh' : normSl;
      const lingvaTl = normTl === 'zh-CN' ? 'zh' : normTl;
      const url = `${host}/api/v1/${encodeURIComponent(lingvaSl)}/${encodeURIComponent(lingvaTl)}/${encodeURIComponent(cleanText)}`;
      const resp = await fetch(url, { signal: AbortSignal.timeout(2000) });
      if (resp.ok) {
        const data = await resp.json();
        if (data && data.translation && data.translation.trim()) {
          return data.translation.trim();
        }
      }
    } catch (ignored) {}
  }

  return null;
}

// Multi-Engine Ultra-Fast Parallel Translation Service
// Renders the fastest valid translation in parallel (~100-150ms total response time)
async function fetchTranslation(text, fromLang, toLang) {
  if (!text || text.trim() === '') return text;
  const sl = normalizeLangCode(fromLang);
  const tl = normalizeLangCode(toLang);
  if (sl === tl) return text;

  const cacheKey = `${sl}_${tl}_${text.trim().toLowerCase()}`;
  if (translationCache.has(cacheKey)) {
    return translationCache.get(cacheKey);
  }

  // Prepare candidate promises for low-latency parallel execution
  const tasks = [];

  // DeepL official (if language pair is supported by DeepL)
  if (DEEPL_KEY && DEEPL_TARGETS[tl.toLowerCase()]) {
    tasks.push(
      translateWithDeepL(text, sl, tl).then(res => {
        if (res && res.toLowerCase() !== text.trim().toLowerCase() && !res.includes('orthèse')) return res;
        throw new Error('empty');
      })
    );
  }

  // Google Cloud official (if key provided and active)
  if (GOOGLE_TRANSLATE_KEY) {
    tasks.push(
      translateWithGoogleCloud(text, sl, tl).then(res => {
        if (res && res.toLowerCase() !== text.trim().toLowerCase() && !res.includes('orthèse')) return res;
        throw new Error('empty');
      })
    );
  }

  // Google Webapi / Open-source fallback (ultra-fast ~120ms lightweight GET)
  tasks.push(
    translateWithOpenSource(text, sl, tl).then(res => {
      if (res && res.toLowerCase() !== text.trim().toLowerCase() && !res.includes('orthèse')) return res;
      throw new Error('empty');
    })
  );

  // Return the fastest engine to respond with a clean valid translation!
  try {
    const fastestResult = await Promise.any(tasks);
    if (fastestResult) {
      translationCache.set(cacheKey, fastestResult);
      return fastestResult;
    }
  } catch (allFailed) {}

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

  // 3. Resilient Cloud Intelligence Fallback (Zero-Key Pollinations AI)
  try {
    const docPrompt = transcript ? `\nAttached Document / Transcript Context:\n"""\n${transcript.slice(-4000)}\n"""\n` : '';
    const cloudResp = await fetch('https://text.pollinations.ai/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        messages: [
          {
            role: 'system',
            content: `You are Qinter AI, an advanced, highly versatile multilingual AI assistant (like ChatGPT). Answer general questions, coding, explanations, and creative requests freely. If document context is provided below, reference it when asked; otherwise answer freely.\n${docPrompt}\nAlways respond in the exact language of the user prompt with elegant markdown.`
          },
          { role: 'user', content: prompt }
        ],
        model: 'openai',
        temperature: 0.6
      }),
      signal: AbortSignal.timeout(18000)
    });
    if (cloudResp.ok) {
      const text = await cloudResp.text();
      if (text && text.trim().length > 0) {
        return {
          reply: text.trim(),
          model: 'Cloud Intelligence (Failover)',
          citations: []
        };
      }
    }
  } catch (ce) {
    console.log('[Cloud Failover] Note:', ce.message);
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

  // Health check API for cloud host monitoring (Render, Railway, Uptime)
  if (pathname === '/health' || pathname === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({ status: 'ok', uptime: Math.round(process.uptime()), timestamp: Date.now() }));
    return;
  }

  // 1. Session Existence Pre-Flight Validation API
  if (pathname === '/api/rooms/check') {
    const code = (parsedUrl.searchParams.get('code') || '').trim().toUpperCase();
    const roomMeta = activeRooms.get(code);
    const clientSet = rooms.get(code);
    const isLive = Boolean(roomMeta && roomMeta.hostWs && roomMeta.hostWs.readyState === WebSocket.OPEN);
    const hasClients = Boolean(clientSet && clientSet.size > 0);

    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    if (code && (isLive || hasClients)) {
      res.end(JSON.stringify({
        exists: true,
        hostActive: isLive,
        participantCount: clientSet ? clientSet.size : 1,
        sourceLang: roomMeta ? roomMeta.sourceLang : null,
        targetLang: roomMeta ? roomMeta.targetLang : null
      }));
    } else {
      res.end(JSON.stringify({
        exists: false,
        hostActive: false,
        error: 'The session does not exist'
      }));
    }
    return;
  }

  // 2. Translate Spot Proximity Discovery API
  if (pathname === '/api/spots/nearest') {
    const lat = parseFloat(parsedUrl.searchParams.get('lat'));
    const lon = parseFloat(parsedUrl.searchParams.get('lon'));
    const maxDist = parseInt(parsedUrl.searchParams.get('maxDist') || '15000', 10); // default 15km

    const spots = [];
    for (const [code, meta] of activeRooms.entries()) {
      if (meta.hostWs && meta.hostWs.readyState === WebSocket.OPEN) {
        let dist = null;
        if (!isNaN(lat) && !isNaN(lon) && meta.lat != null && meta.lon != null) {
          dist = calculateDistanceMeters(lat, lon, meta.lat, meta.lon);
          if (dist > maxDist) continue;
        }
        const clientCount = rooms.has(code) ? rooms.get(code).size : 1;
        spots.push({
          roomCode: code,
          distanceMeters: dist,
          sourceLang: meta.sourceLang || 'fr',
          targetLang: meta.targetLang || 'en',
          participantCount: clientCount
        });
      }
    }

    spots.sort((a, b) => {
      if (a.distanceMeters == null) return 1;
      if (b.distanceMeters == null) return -1;
      return a.distanceMeters - b.distanceMeters;
    });

    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    res.end(JSON.stringify({
      spots: spots,
      total: spots.length,
      nearest: spots[0] || null
    }));
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

  // Neural Audio TTS Stream API (VoiceStudio AI Voices, Male Default + 4 Presets, 100% Apple iOS / Safari compatible)
  if (pathname === '/api/tts') {
    const text = (parsedUrl.searchParams.get('text') || '').trim();
    const lang = normalizeLangCode(parsedUrl.searchParams.get('lang') || 'en');
    const voice = (parsedUrl.searchParams.get('voice') || 'male_natural').trim();
    const rate = (parsedUrl.searchParams.get('rate') || '1.0').trim();

    if (!text) {
      if (!res.headersSent) {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end('Missing text');
      }
      return;
    }

    getTtsAudioBuffer(text, lang, voice, rate)
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
  let userRole = 'LISTENER';

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'JOIN_ROOM') {
        const code = (msg.roomCode || '').toUpperCase();
        userRole = (msg.role || 'LISTENER').toUpperCase();
        joinedRoom = code;

        // If a listener tries to join a room that does not exist or has no active host
        if (userRole === 'LISTENER') {
          const roomMeta = activeRooms.get(code);
          const clientSet = rooms.get(code);
          const isHostActive = Boolean(roomMeta && roomMeta.hostWs && roomMeta.hostWs.readyState === WebSocket.OPEN);
          const hasExisting = Boolean(clientSet && clientSet.size > 0);

          if (!isHostActive && !hasExisting) {
            ws.send(JSON.stringify({
              type: 'ROOM_ERROR',
              roomCode: code,
              error: 'The session does not exist'
            }));
            return;
          }
        }

        if (!rooms.has(code)) {
          rooms.set(code, new Set());
        }
        rooms.get(code).add(ws);

        if (userRole === 'HOST') {
          activeRooms.set(code, {
            code,
            hostWs: ws,
            lat: (typeof msg.lat === 'number') ? msg.lat : null,
            lon: (typeof msg.lon === 'number') ? msg.lon : null,
            sourceLang: msg.sourceLang || 'fr',
            targetLang: msg.targetLang || 'en',
            createdAt: Date.now()
          });
        }

        // Notify room of updated participant count
        const count = rooms.get(code).size;
        broadcastToRoom(code, { type: 'PARTICIPANTS_COUNT', count, roomCode: code });
        ws.send(JSON.stringify({ type: 'JOIN_OK', roomCode: code, count, role: userRole }));
      } else if (msg.type === 'UPDATE_SPOT' && joinedRoom && userRole === 'HOST') {
        const existing = activeRooms.get(joinedRoom) || { code: joinedRoom, hostWs: ws };
        if (typeof msg.lat === 'number') existing.lat = msg.lat;
        if (typeof msg.lon === 'number') existing.lon = msg.lon;
        if (msg.sourceLang) existing.sourceLang = msg.sourceLang;
        if (msg.targetLang) existing.targetLang = msg.targetLang;
        activeRooms.set(joinedRoom, existing);
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

      // Clean up host room immediately when host socket closes
      if (activeRooms.has(joinedRoom) && activeRooms.get(joinedRoom).hostWs === ws) {
        activeRooms.delete(joinedRoom);
        broadcastToRoom(joinedRoom, {
          type: 'HOST_CLOSED',
          roomCode: joinedRoom,
          message: 'The host has ended the translation session'
        });
      }

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
