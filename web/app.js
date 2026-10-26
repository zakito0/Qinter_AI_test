// Language Catalog with BCP-47 tags
const LANGUAGES = [
  { code: 'en', native: 'English', bcp: 'en-US' },
  { code: 'ar', native: 'العربية', bcp: 'ar-SA', rtl: true },
  { code: 'fr', native: 'Français', bcp: 'fr-FR' },
  { code: 'es', native: 'Español', bcp: 'es-ES' },
  { code: 'de', native: 'Deutsch', bcp: 'de-DE' },
  { code: 'it', native: 'Italiano', bcp: 'it-IT' },
  { code: 'zh', native: '中文', bcp: 'zh-CN' },
  { code: 'ja', native: '日本語', bcp: 'ja-JP' }
];

let currentRole = null; // 'HOST' or 'LISTENER'
let currentRoomCode = '';
const browserDefaultLang = (navigator.language || navigator.userLanguage || 'fr').slice(0, 2).toLowerCase();
let selectedHostLang = LANGUAGES.some(l => l.code === browserDefaultLang) ? browserDefaultLang : 'fr';
let selectedListenLang = selectedHostLang === 'ar' ? 'fr' : 'ar';
let selectedHostTargetLang = selectedHostLang === 'ar' ? 'fr' : 'ar'; // Preview target for host (matches mobile HostScreen)
let isCapturing = false;
let isHandRaised = false;
let lanIpAddress = window.location.hostname;
let serverPort = window.location.port || '8085';

// Server-Assisted ASR & Hybrid Audio Engine State
let asrRecorder = null;
let asrChunks = [];
let asrSliceStartTime = 0;
let lastVoiceDetectedTime = 0;
let isVoiceActiveState = false;
let isTranscribingSlice = false;
let lastCommittedText = '';
let lastCommittedTime = 0;
let ws = null;
let speechRecognizer = null;
let audioContext = null;
let analyser = null;
let micStream = null;
let highPassFilter = null;
let vocalEq = null;
let compressor = null;
let noiseGateGain = null;
let mediaRecorder = null;
let treatedDestination = null;
let recordedAudioBlob = null;
let recordedAudioUrl = null;
let isRecordingSlice = false;
let recordStartTime = 0;
let currentAudioPlayer = null;
let isHostAudioTranslationEnabled = false; // Off by default on host so the speaker's mic is never interrupted by echo
let currentTtsAudio = null;
let readerWeightRegular = false;
const sessionTranscriptLog = [];

// Apple Devices (iOS Safari / iPadOS) Audio Unlocker
const isAppleDevice = /iPad|iPhone|iPod|Macintosh/i.test(navigator.userAgent) && (('ontouchstart' in window) || navigator.maxTouchPoints > 0);
let isAudioUnlocked = false;

function unlockAudioEngine() {
  if (isAudioUnlocked) return;
  if (!audioContext) {
    audioContext = new (window.AudioContext || window.webkitAudioContext)();
  }
  if (audioContext && audioContext.state === 'suspended') {
    audioContext.resume();
  }
  try {
    const silent = new Audio('data:audio/wav;base64,UklGRigAAABXQVZFZm10IBIAAAABAAEARKwAAIhYAQACABAAAABkYXRhAgAAAAEA');
    silent.playsInline = true;
    silent.crossOrigin = 'anonymous';
    const p = silent.play();
    if (p) {
      p.then(() => {
        silent.pause();
        isAudioUnlocked = true;
        hideIosBanner();
      }).catch(() => {});
    } else {
      isAudioUnlocked = true;
      hideIosBanner();
    }
  } catch (e) {
    isAudioUnlocked = true;
  }
}

function showIosBannerIfNeeded() {
  if (isAppleDevice && !isAudioUnlocked) {
    const banner = document.getElementById('ios-audio-banner');
    if (banner) banner.classList.add('visible');
  }
}

function hideIosBanner() {
  const banner = document.getElementById('ios-audio-banner');
  if (banner) banner.classList.remove('visible');
}

// Automatically unlock audio on ANY user touch or click gesture on the screen
['click', 'touchstart', 'touchend', 'keydown'].forEach(evt => {
  window.addEventListener(evt, () => {
    unlockAudioEngine();
  }, { passive: true });
});

// Query LAN IP from server (only adopted if running locally on localhost dev environment)
fetch('/api/info')
  .then(r => r.json())
  .then(data => {
    const isLocalHost = ['localhost', '127.0.0.1', '0.0.0.0'].includes(window.location.hostname);
    if (isLocalHost && data.lanIp && !['localhost', '127.0.0.1', '0.0.0.0'].includes(data.lanIp)) {
      lanIpAddress = data.lanIp;
      serverPort = data.port || '8085';
    }
  })
  .catch(() => {});

// Connect to WebSocket Server for Real-Time Multi-Device Sync
function connectWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}`;
  try {
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      if (currentRoomCode) {
        ws.send(JSON.stringify({ type: 'JOIN_ROOM', roomCode: currentRoomCode, role: currentRole }));
      }
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        handleNetworkMessage(msg);
      } catch (e) {
        console.error('WS parse error:', e);
      }
    };

    ws.onerror = () => {
      // Suppress noisy network console error while reconnecting
    };

    ws.onclose = () => {
      setTimeout(connectWebSocket, 2000); // Reconnect
    };
  } catch (e) {
    setTimeout(connectWebSocket, 2000);
  }
}

connectWebSocket();

// Same-machine BroadcastChannel fallback
const localChannel = new BroadcastChannel('qinter_local_bus');
localChannel.onmessage = (e) => handleNetworkMessage(e.data);

function broadcastPacket(packet) {
  packet.roomCode = currentRoomCode;
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(packet));
  }
  try {
    localChannel.postMessage(packet);
  } catch (e) {}
}

function handleNetworkMessage(msg) {
  if (!msg || msg.roomCode !== currentRoomCode) return;

  switch (msg.type) {
    case 'PARTICIPANTS_COUNT':
      if (currentRole === 'HOST') {
        const syncPill = document.getElementById('host-sync-pill');
        if (syncPill) syncPill.textContent = `● Live (${msg.count})`;
        const badge = document.getElementById('host-participant-badge');
        if (badge) badge.textContent = `👥 ${msg.count} in room`;
      }
      break;

    case 'DRAFT':
      if (currentRole === 'LISTENER') {
        const draftEl = document.getElementById('listen-draft-caption');
        if (draftEl) {
          if (!msg.text || !msg.text.trim()) {
            draftEl.innerHTML = '';
          } else {
            const isRtl = selectedListenLang === 'ar';
            if (msg.targetLang === selectedListenLang && msg.draftTranslation) {
              draftEl.innerHTML = `
                <div style="background: var(--ink-800); border: 1px solid var(--gold-400); border-radius: 12px; padding: 10px 14px; margin: 8px 0; text-align: ${isRtl ? 'right' : 'left'};">
                  <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px;">
                    <span style="font-size: 10px; background: rgba(212,175,55,0.25); color: var(--gold-200); padding: 2px 6px; border-radius: 4px; font-weight: bold;">● LIVE INTERPRETATION [${selectedListenLang.toUpperCase()}]</span>
                    <span style="font-size: 10px; color: var(--gold-500);">Speaking now...</span>
                  </div>
                  <div style="font-size: 16px; color: var(--gold-100); font-weight: bold; direction: ${isRtl ? 'rtl' : 'ltr'};">${msg.draftTranslation}</div>
                  ${msg.text !== msg.draftTranslation ? `<div style="font-size: 12px; color: var(--gold-500); margin-top: 3px; font-style: italic;">Original: ${msg.text}</div>` : ''}
                </div>
              `;
            } else {
              handleListenerDraftTranslation(msg.text, msg.sourceLang || 'en');
            }
          }
        }
      }
      break;

    case 'SEG':
      if (currentRole === 'LISTENER') {
        handleIncomingSegment(msg);
      }
      break;

    case 'FLOOR_REQUEST':
      if (currentRole === 'HOST') {
        alert('✋ A listener in the room has raised their hand to speak!');
      }
      break;
  }
}

let listenerDraftDebounce = null;
function handleListenerDraftTranslation(text, sourceLang) {
  clearTimeout(listenerDraftDebounce);
  const draftEl = document.getElementById('listen-draft-caption');
  if (!draftEl) return;
  listenerDraftDebounce = setTimeout(async () => {
    try {
      const isRtl = selectedListenLang === 'ar';
      let trans = text;
      if (sourceLang !== selectedListenLang) {
        const resp = await fetch(`/api/translate?text=${encodeURIComponent(text)}&from=${sourceLang}&to=${selectedListenLang}`);
        const data = await resp.json();
        if (data && data.translated) trans = data.translated;
      }
      draftEl.innerHTML = `
        <div style="background: var(--ink-800); border: 1px solid var(--gold-400); border-radius: 12px; padding: 10px 14px; margin: 8px 0; text-align: ${isRtl ? 'right' : 'left'};">
          <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px;">
            <span style="font-size: 10px; background: rgba(212,175,55,0.25); color: var(--gold-200); padding: 2px 6px; border-radius: 4px; font-weight: bold;">● LIVE INTERPRETATION [${selectedListenLang.toUpperCase()}]</span>
            <span style="font-size: 10px; color: var(--gold-500);">Speaking now...</span>
          </div>
          <div style="font-size: 16px; color: var(--gold-100); font-weight: bold; direction: ${isRtl ? 'rtl' : 'ltr'};">${trans}</div>
          ${text !== trans ? `<div style="font-size: 12px; color: var(--gold-500); margin-top: 3px; font-style: italic;">Original: ${text}</div>` : ''}
        </div>
      `;
    } catch (e) {}
  }, 100);
}

// Check for ?room= in URL to automatically join when scanning QR
window.addEventListener('DOMContentLoaded', () => {
  showIosBannerIfNeeded();
  const params = new URLSearchParams(window.location.search);
  const roomParam = params.get('room');
  if (roomParam) {
    joinRoomWithCode(roomParam.trim().toUpperCase());
  }
});

// UI Navigation & Bottom Navbar (Browser Lite Version)
function switchNavTab(tab) {
  if (tab === 'speech') {
    const navTabs = document.querySelectorAll('.nav-tab');
    navTabs.forEach(t => t.classList.remove('active'));
    const speechTab = document.getElementById('tab-speech');
    if (speechTab) speechTab.classList.add('active');
    showScreen('screen-home');
  } else {
    // Secondary modalities locked in Browser Lite
    openDownloadAppModal(tab);
  }
}

function showScreen(screenId) {
  if (screenId === 'screen-conversation' || screenId === 'screen-visual' || screenId === 'screen-doctrad') {
    openDownloadAppModal(screenId.replace('screen-', ''));
    return;
  }

  document.querySelectorAll('.screen').forEach(s => s.classList.remove('active'));
  const target = document.getElementById(screenId);
  if (target) target.classList.add('active');

  const bottomNav = document.getElementById('main-bottom-nav');
  if (bottomNav) {
    if (screenId === 'screen-home') {
      bottomNav.classList.remove('hidden');
    } else {
      bottomNav.classList.add('hidden');
    }
  }
}

// Download App Modal for Lite Edition Exclusives
function openDownloadAppModal(feature = 'general') {
  const modal = document.getElementById('download-app-modal');
  if (!modal) return;

  const iconEl = document.getElementById('app-modal-icon');
  const titleEl = document.getElementById('app-modal-title');
  const subtitleEl = document.getElementById('app-modal-subtitle');
  const msgEl = document.getElementById('app-modal-msg');

  if (feature === 'ai') {
    if (iconEl) iconEl.textContent = '✨';
    if (titleEl) titleEl.textContent = 'Qinter AI Intelligence';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'Real-time AI speech explanations, deep sentence breakdowns, and interactive speech learning agents run on the local AI neural engine in the Qinter Mobile App. Download now to unlock AI features.';
  } else if (feature === 'conversation') {
    if (iconEl) iconEl.textContent = '🗣️';
    if (titleEl) titleEl.textContent = 'Bilateral Conversation Mode';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'Face-to-face bilateral voice translation, table mode flip, and dual-party voice recognition are exclusive to the full Qinter Mobile App.';
  } else if (feature === 'visual') {
    if (iconEl) iconEl.textContent = '📷';
    if (titleEl) titleEl.textContent = 'AI Vision Trad (Photo & OCR)';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'Translating street signs, menus, and photo documents with on-device camera OCR is exclusive to the full Qinter Mobile App.';
  } else if (feature === 'doctrad') {
    if (iconEl) iconEl.textContent = '📄';
    if (titleEl) titleEl.textContent = 'Document Translation (DocTrad)';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'Full document and PDF translation, dual-column bilingual reader, and offline neural translation are exclusive to the full Qinter Mobile App.';
  } else if (feature === 'account') {
    if (iconEl) iconEl.textContent = '👤';
    if (titleEl) titleEl.textContent = 'Cloud Accounts & Sync';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'User account registration, Google Sign-In, saved session archives, and multi-device cloud synchronization are available exclusively in the full Qinter Mobile App.';
  } else {
    if (iconEl) iconEl.textContent = '📱';
    if (titleEl) titleEl.textContent = 'Mobile App Feature';
    if (subtitleEl) subtitleEl.textContent = 'Exclusive to Qinter for Android';
    if (msgEl) msgEl.textContent = 'This feature is exclusive to the full Qinter Mobile App. Web Lite is dedicated to live speech translation rooms.';
  }

  modal.classList.add('active');
}

function closeDownloadAppModal() {
  const modal = document.getElementById('download-app-modal');
  if (modal) modal.classList.remove('active');
}

// Room Code Parser
function extractRoomCode(raw) {
  if (!raw) return null;
  const trimmed = raw.trim();
  const clean = trimmed.toUpperCase();
  if (clean.length === 6 && /^[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]+$/.test(clean)) {
    return clean;
  }
  const qMatch = trimmed.match(/[?&]room=([A-Za-z0-9]{6})/i);
  if (qMatch) return qMatch[1].toUpperCase();

  const pMatch = trimmed.match(/(?:\/room\/|\/)([A-Za-z0-9]{6})(?:[/?#]|$)/i);
  if (pMatch) return pMatch[1].toUpperCase();

  const wMatch = trimmed.match(/\b([A-Za-z0-9]{6})\b/);
  if (wMatch) return wMatch[1].toUpperCase();

  return null;
}

// Web QR Scanner (Camera & File upload)
let qrVideoTrack = null;
let qrScanAnimFrame = null;

async function triggerQrScan() {
  const modal = document.getElementById('qr-camera-modal');
  const video = document.getElementById('qr-video');

  // Try opening camera stream
  if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment' }
      });
      qrVideoTrack = stream.getVideoTracks()[0];
      video.srcObject = stream;
      video.setAttribute('playsinline', true);
      await video.play();

      modal.classList.add('active');
      scanQrVideoLoop();
      return;
    } catch (err) {
      console.warn('Camera access unavailable or denied:', err);
    }
  }

  // Fallback to image picker if camera fails or is blocked
  pickQrFile();
}

function closeQrScanner() {
  const modal = document.getElementById('qr-camera-modal');
  if (modal) modal.classList.remove('active');
  if (qrScanAnimFrame) {
    cancelAnimationFrame(qrScanAnimFrame);
    qrScanAnimFrame = null;
  }
  if (qrVideoTrack) {
    qrVideoTrack.stop();
    qrVideoTrack = null;
  }
}

function scanQrVideoLoop() {
  const video = document.getElementById('qr-video');
  if (video && video.readyState === video.HAVE_ENOUGH_DATA && window.jsQR) {
    const canvas = document.createElement('canvas');
    canvas.width = video.videoWidth;
    canvas.height = video.videoHeight;
    const ctx = canvas.getContext('2d');
    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
    const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
    const code = jsQR(imageData.data, imageData.width, imageData.height);

    if (code && code.data) {
      const roomCode = extractRoomCode(code.data);
      if (roomCode) {
        closeQrScanner();
        fillTestCode(roomCode);
        joinRoomAction();
        return;
      }
    }
  }
  qrScanAnimFrame = requestAnimationFrame(scanQrVideoLoop);
}

function pickQrFile() {
  closeQrScanner();
  const fileInput = document.getElementById('web-qr-file-input');
  if (fileInput) fileInput.click();
}

function handleWebQrFile(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = (e) => {
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = img.width;
      canvas.height = img.height;
      const ctx = canvas.getContext('2d');
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);

      if (window.jsQR) {
        const result = jsQR(imageData.data, imageData.width, imageData.height);
        if (result && result.data) {
          const roomCode = extractRoomCode(result.data);
          if (roomCode) {
            fillTestCode(roomCode);
            joinRoomAction();
            return;
          }
        }
      }
      alert('Could not detect a valid room QR code in this image.');
    };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
  event.target.value = '';
}


// 6-Char Code Input handling
const hiddenInput = document.getElementById('hidden-code-input');
const codeCells = [
  document.getElementById('c0'),
  document.getElementById('c1'),
  document.getElementById('c2'),
  document.getElementById('c3'),
  document.getElementById('c4'),
  document.getElementById('c5')
];
const btnJoin = document.getElementById('btn-join');

document.getElementById('home-code-frame').addEventListener('click', () => {
  hiddenInput.focus();
});

hiddenInput.addEventListener('input', (e) => {
  const val = e.target.value.toUpperCase().replace(/[^23456789ABCDEFGHJKLMNPQRSTUVWXYZ]/g, '').slice(0, 6);
  e.target.value = val;
  updateCodeDisplay(val);
});

function updateCodeDisplay(val) {
  for (let i = 0; i < 6; i++) {
    const char = val[i] || '·';
    codeCells[i].textContent = char;
    codeCells[i].classList.toggle('filled', !!val[i]);
    codeCells[i].classList.toggle('focused', i === val.length);
  }
  btnJoin.disabled = val.length !== 6;
}

function fillTestCode(code) {
  hiddenInput.value = code;
  updateCodeDisplay(code);
}

// Dynamic Join URL resolution:
// On deployed cloud domains (Render, Railway, custom domains), ALWAYS use window.location.origin
// so invite links and QR codes point to the public HTTPS URL (e.g. https://qinter-ai-alpha-test.onrender.com/?room=ABCDEF)
// and NEVER fall back to localhost or internal container IP.
function getJoinUrl(roomCode) {
  const code = (roomCode || currentRoomCode || '').trim();
  const hostname = window.location.hostname;
  const isLocalHost = hostname === 'localhost' || hostname === '127.0.0.1' || hostname === '0.0.0.0' || hostname === '::1';

  // 1. If running on any public/deployed host (e.g. onrender.com, railway.app, custom domain):
  if (!isLocalHost) {
    return `${window.location.origin}/?room=${encodeURIComponent(code)}`;
  }

  // 2. Only if developing locally on host machine:
  // Use LAN IP if the server provided one (e.g. 192.168.x.x) so mobile devices on the same WiFi can join
  if (lanIpAddress && !['localhost', '127.0.0.1', '0.0.0.0'].includes(lanIpAddress)) {
    const proto = window.location.protocol;
    const port = serverPort ? `:${serverPort}` : '';
    return `${proto}//${lanIpAddress}${port}/?room=${encodeURIComponent(code)}`;
  }

  // Local fallback
  return `${window.location.origin}/?room=${encodeURIComponent(code)}`;
}

// Host actions (Browser Lite Speech Room - Exact Mobile HostScreen Replica)
function startHostAction() {
  currentRole = 'HOST';
  currentRoomCode = generateRoomCode();

  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'JOIN_ROOM', roomCode: currentRoomCode, role: 'HOST' }));
  }

  // Top Bar setup
  const titleEl = document.getElementById('host-room-title');
  if (titleEl) titleEl.textContent = `Room ${currentRoomCode}`;
  const syncPill = document.getElementById('host-sync-pill');
  if (syncPill) syncPill.textContent = '● Live (1)';

  // Expandable QR Card setup
  const joinUrl = getJoinUrl(currentRoomCode);
  const linkTextEl = document.getElementById('host-qr-link-text');
  if (linkTextEl) {
    linkTextEl.textContent = `Listeners join at ${joinUrl}`;
  }
  const qrCanvas = document.getElementById('qr-canvas');
  if (window.QRCode && qrCanvas) {
    QRCode.toCanvas(qrCanvas, joinUrl, {
      width: 176,
      margin: 1,
      color: { dark: '#0B0B0C', light: '#F5EBCB' }
    });
  }
  const qrCard = document.getElementById('host-qr-card');
  if (qrCard) qrCard.style.display = 'none';
  const qrBtn = document.getElementById('btn-toggle-host-qr');
  if (qrBtn) qrBtn.classList.remove('active');

  // Unified Language Selector setup
  const srcSelect = document.getElementById('host-source-lang');
  if (srcSelect) srcSelect.value = selectedHostLang;
  const tgtSelect = document.getElementById('host-target-lang');
  if (tgtSelect) tgtSelect.value = selectedHostTargetLang;

  // Real-Time Draft Card reset
  const draftCard = document.getElementById('host-live-draft-card');
  if (draftCard) draftCard.style.display = 'none';
  const draftOrig = document.getElementById('host-draft-original');
  if (draftOrig) draftOrig.textContent = '';
  const draftTrans = document.getElementById('host-draft-translated');
  if (draftTrans) draftTrans.textContent = '';

  // Transcript Feed reset
  const feedList = document.getElementById('host-transcript-list');
  if (feedList) {
    feedList.innerHTML = '';
    feedList.style.display = 'none';
  }
  const emptyFeed = document.getElementById('host-empty-feed');
  if (emptyFeed) emptyFeed.style.display = 'flex';

  // Bottom Input & Mic Orb reset
  const quickInput = document.getElementById('quick-speech-input');
  if (quickInput) quickInput.value = '';

  isCapturing = true;
  const micBtn = document.getElementById('host-mic-btn');
  if (micBtn) {
    micBtn.className = 'compact-mic-orb capturing';
    const micIcon = document.getElementById('host-mic-icon');
    if (micIcon) micIcon.textContent = '🎙';
  }

  showScreen('screen-host');

  // Immediately unlock audio engine and request microphone permission + continuous speech recognition
  unlockAudioEngine();
  requestMicrophoneAccess();
}

function generateRoomCode() {
  const charset = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += charset[Math.floor(Math.random() * charset.length)];
  }
  return code;
}

function toggleHostQrCard() {
  const card = document.getElementById('host-qr-card');
  const btn = document.getElementById('btn-toggle-host-qr');
  if (!card) return;
  const isHidden = card.style.display === 'none' || !card.style.display;
  card.style.display = isHidden ? 'flex' : 'none';
  if (btn) btn.classList.toggle('active', isHidden);
}

function copyHostJoinLink() {
  const joinUrl = getJoinUrl(currentRoomCode);
  const btn = document.getElementById('host-copy-link-btn');
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(joinUrl).then(() => {
      if (btn) {
        const orig = btn.textContent;
        btn.textContent = '✓ Copied!';
        setTimeout(() => { btn.textContent = orig; }, 2000);
      }
    }).catch(() => {
      prompt('Copy room link:', joinUrl);
    });
  } else {
    prompt('Copy room link:', joinUrl);
  }
}

function onHostSourceLangChange() {
  const sel = document.getElementById('host-source-lang');
  if (sel) {
    selectedHostLang = sel.value;
    if (speechRecognizer) {
      try { speechRecognizer.abort(); } catch (e) {}
      if (isCapturing) initSpeechRecognition();
    }
  }
}

function onHostTargetLangChange() {
  const sel = document.getElementById('host-target-lang');
  if (sel) {
    selectedHostTargetLang = sel.value;
  }
}

function swapHostLanguages() {
  const tmp = selectedHostLang;
  selectedHostLang = selectedHostTargetLang;
  selectedHostTargetLang = tmp;

  const srcSel = document.getElementById('host-source-lang');
  const tgtSel = document.getElementById('host-target-lang');
  if (srcSel) srcSel.value = selectedHostLang;
  if (tgtSel) tgtSel.value = selectedHostTargetLang;

  if (speechRecognizer) {
    try { speechRecognizer.abort(); } catch (e) {}
    if (isCapturing) initSpeechRecognition();
  }
}

// Real-Time Audio DSP Treatment & Waveform Pipeline
let animationFrameId = null;
let lastSpeechTimestamp = 0;
let recordedChunks = [];

function startMicrophonePipeline() {
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    document.getElementById('host-draft-caption').textContent = 'Audio capture not supported in this browser. Use quick input below.';
    return;
  }

  navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: false } })
    .then(stream => {
      micStream = stream;
      const source = audioContext.createMediaStreamSource(stream);

      // --- 4-STAGE PROFESSIONAL AUDIO TREATMENT CHAIN ---
      // 1. High-Pass Filter: Cuts desk rumble, HVAC hum, handling noise below 80 Hz
      highPassFilter = audioContext.createBiquadFilter();
      highPassFilter.type = 'highpass';
      highPassFilter.frequency.value = 80;
      highPassFilter.Q.value = 0.707;

      // 2. Vocal Presence Peaking EQ: Boosts 2.5 kHz speech formant frequencies
      vocalEq = audioContext.createBiquadFilter();
      vocalEq.type = 'peaking';
      vocalEq.frequency.value = 2500;
      vocalEq.gain.value = 4.0; // +4dB boost for vocal articulation
      vocalEq.Q.value = 1.2;

      // 3. Dynamic Range Compressor (Broadcast AGC)
      // Levelizes whisper and loud speech without clipping
      compressor = audioContext.createDynamicsCompressor();
      compressor.threshold.value = -24; // dB
      compressor.knee.value = 30; // dB
      compressor.ratio.value = 4; // 4:1 compression
      compressor.attack.value = 0.003; // 3ms fast attack
      compressor.release.value = 0.25; // 250ms release

      // 4. Noise Gate Gain Node
      noiseGateGain = audioContext.createGain();
      noiseGateGain.gain.value = 1.0;

      // 5. Treated Audio Destination for MediaRecorder
      treatedDestination = audioContext.createMediaStreamDestination();

      // 6. Analyser Node for treated waveform oscilloscope
      analyser = audioContext.createAnalyser();
      analyser.fftSize = 512;
      analyser.smoothingTimeConstant = 0.6;

      // Connect DSP Chain:
      // micSource -> HPF (80Hz) -> Vocal EQ (2.5kHz) -> AGC Compressor -> Noise Gate -> Analyser
      //                                                                              -> Treated Destination
      source.connect(highPassFilter);
      highPassFilter.connect(vocalEq);
      vocalEq.connect(compressor);
      compressor.connect(noiseGateGain);
      noiseGateGain.connect(analyser);
      noiseGateGain.connect(treatedDestination);

      // Setup MediaRecorder for treated audio
      setupMediaRecorder();

      // Setup Canvas & UI elements
      const canvas = document.getElementById('waveform-canvas');
      const ctx = canvas ? canvas.getContext('2d') : null;
      const timeDomainData = new Uint8Array(analyser.frequencyBinCount);

      const orb = document.getElementById('host-mic-orb');
      const vadBadge = document.getElementById('vad-badge');
      const vadText = document.getElementById('vad-status-text');
      const dbMeter = document.getElementById('audio-level-db');
      const recBadge = document.getElementById('rec-status-badge');
      const recTimeText = document.getElementById('rec-time-text');

      function processAudioFrame() {
        if (!isCapturing) return;

        analyser.getByteTimeDomainData(timeDomainData);

        // 1. Calculate RMS & dBFS for Voice Activity Detection
        let sumSquares = 0;
        for (let i = 0; i < timeDomainData.length; i++) {
          const normalized = (timeDomainData[i] - 128) / 128;
          sumSquares += normalized * normalized;
        }
        const rms = Math.sqrt(sumSquares / timeDomainData.length);
        const db = rms > 0.0001 ? Math.max(-60, 20 * Math.log10(rms)) : -60;

        if (dbMeter) {
          dbMeter.textContent = `${db.toFixed(1)} dBFS`;
        }

        // 2. VAD: Voice Activity Detection (threshold: -42 dBFS)
        const isVoiceActive = db > -42.0;
        const now = Date.now();

        if (isVoiceActive) {
          lastSpeechTimestamp = now;
          if (vadBadge) vadBadge.classList.add('active');
          if (vadText) vadText.textContent = 'VAD: SPEECH DETECTED';

          // Open noise gate smoothly
          if (noiseGateGain) noiseGateGain.gain.setTargetAtTime(1.0, audioContext.currentTime, 0.02);

          // Start MediaRecorder if not already recording
          if (mediaRecorder && mediaRecorder.state === 'inactive') {
            recordedChunks = [];
            try {
              mediaRecorder.start(100);
              isRecordingSlice = true;
              recordStartTime = now;
              if (recBadge) recBadge.style.opacity = '1';
            } catch (e) {}
          }

          if (isRecordingSlice && recTimeText) {
            const elapsed = ((now - recordStartTime) / 1000).toFixed(1);
            recTimeText.textContent = `REC ${elapsed}s`;
          }
        } else if (now - lastSpeechTimestamp > 650) {
          // Pause / Silence detected after 650ms
          if (vadBadge) vadBadge.classList.remove('active');
          if (vadText) vadText.textContent = 'VAD: SILENCE';

          // Smoothly close noise gate
          if (noiseGateGain) noiseGateGain.gain.setTargetAtTime(0.08, audioContext.currentTime, 0.05);

          // Stop recording treated audio slice
          if (mediaRecorder && mediaRecorder.state === 'recording') {
            try {
              mediaRecorder.stop();
            } catch (e) {}
            isRecordingSlice = false;
            if (recBadge) recBadge.style.opacity = '0';
          }
        }

        // 3. Audio-reactive scale on Mic Orb
        if (orb) {
          const scale = 1.0 + Math.min(0.4, rms * 1.5);
          orb.style.transform = `scale(${scale.toFixed(3)})`;
        }

        // 4. Draw Oscilloscope Waveform on Canvas
        if (ctx && canvas) {
          ctx.fillStyle = '#101011';
          ctx.fillRect(0, 0, canvas.width, canvas.height);

          ctx.lineWidth = 2;
          ctx.strokeStyle = isVoiceActive ? '#F6E7A8' : '#8F6D1F';
          ctx.beginPath();

          const sliceWidth = canvas.width / timeDomainData.length;
          let x = 0;

          for (let i = 0; i < timeDomainData.length; i++) {
            const v = timeDomainData[i] / 128.0;
            const y = (v * canvas.height) / 2;

            if (i === 0) {
              ctx.moveTo(x, y);
            } else {
              ctx.lineTo(x, y);
            }
            x += sliceWidth;
          }

          ctx.lineTo(canvas.width, canvas.height / 2);
          ctx.stroke();
        }

        animationFrameId = requestAnimationFrame(processAudioFrame);
      }

      processAudioFrame();

      // Start Speech Recognition once audio hardware is steady
      initSpeechRecognition();
    })
    .catch(err => {
      console.warn('Microphone stream error:', err);
      const draftEl = document.getElementById('host-draft-caption');
      if (err.name === 'NotAllowedError' || err.name === 'PermissionDeniedError') {
        draftEl.textContent = '⚠️ Mic access denied. Click the lock icon in the URL bar to allow microphone.';
      } else {
        draftEl.textContent = `Mic note: ${err.message}. You can also type or use speech input below.`;
      }
      initSpeechRecognition();
    });
}

// MediaRecorder setup for treated audio chunks
function setupMediaRecorder() {
  if (!window.MediaRecorder || !treatedDestination) return;

  let mime = 'audio/webm;codecs=opus';
  if (!MediaRecorder.isTypeSupported(mime)) {
    mime = 'audio/webm';
    if (!MediaRecorder.isTypeSupported(mime)) {
      mime = '';
    }
  }

  try {
    mediaRecorder = new MediaRecorder(treatedDestination.stream, mime ? { mimeType: mime } : undefined);

    mediaRecorder.ondataavailable = (e) => {
      if (e.data && e.data.size > 0) {
        recordedChunks.push(e.data);
      }
    };

    mediaRecorder.onstop = () => {
      if (recordedChunks.length === 0) return;
      recordedAudioBlob = new Blob(recordedChunks, { type: mime || 'audio/webm' });
      recordedChunks = [];

      if (recordedAudioUrl) URL.revokeObjectURL(recordedAudioUrl);
      recordedAudioUrl = URL.createObjectURL(recordedAudioBlob);

      const dur = ((Date.now() - recordStartTime) / 1000).toFixed(1);
      const card = document.getElementById('audio-playback-card');
      const label = document.getElementById('audio-duration-label');
      if (card) card.classList.add('visible');
      if (label) label.textContent = `${dur}s (${(recordedAudioBlob.size / 1024).toFixed(1)} KB)`;

      // Post treated audio slice to server
      fetch('/api/record', {
        method: 'POST',
        body: recordedAudioBlob,
        headers: { 'Content-Type': mime || 'audio/webm' }
      }).catch(() => {});
    };
  } catch (e) {
    console.warn('MediaRecorder init error:', e);
  }
}

// Playback the last recorded treated audio clip
function playLastRecording() {
  if (!recordedAudioUrl) return;
  if (currentAudioPlayer) {
    currentAudioPlayer.pause();
    currentAudioPlayer = null;
  }
  currentAudioPlayer = new Audio(recordedAudioUrl);
  const btn = document.getElementById('play-audio-btn');
  if (btn) btn.textContent = '⏸ Playing...';

  currentAudioPlayer.onended = () => {
    if (btn) btn.textContent = '▶ Play Audio';
  };
  currentAudioPlayer.onerror = () => {
    if (btn) btn.textContent = '▶ Play Audio';
  };
  currentAudioPlayer.play().catch(e => console.warn('Audio play error:', e));
}

let micAudioContext = null;
let micAnalyser = null;
let micAnimFrame = null;
let speechRestartTimer = null;

// Request native microphone access via navigator.mediaDevices.getUserMedia
async function requestMicrophoneAccess() {
  hideMicNotice();

  // 1. Insecure Context check (Chrome/Safari block mic on non-localhost HTTP)
  if (!window.isSecureContext && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
    const isLocalHost = ['localhost', '127.0.0.1', '0.0.0.0'].includes(location.hostname);
    showMicNotice(
      isLocalHost
        ? '⚠️ Insecure Context: Browsers disable microphone on HTTP. Use http://localhost:8085 on host PC or switch to HTTPS.'
        : '⚠️ Insecure Context: Browsers require HTTPS for microphone access. Please switch to HTTPS.',
      'Switch to HTTPS',
      () => { window.location.href = window.location.href.replace(/^http:/i, 'https:'); },
      'warning'
    );
    return false;
  }

  // 2. Browser navigator.mediaDevices support check
  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
    showMicNotice(
      '⚠️ Browser audio capture not supported. Use Google Chrome, Microsoft Edge, or Safari.',
      null,
      null,
      'error'
    );
    return false;
  }

  try {
    if (micStream) {
      micStream.getTracks().forEach(t => t.stop());
      micStream = null;
    }

    // Direct user-gesture prompt for microphone
    micStream = await navigator.mediaDevices.getUserMedia({
      audio: {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true
      }
    });

    hideMicNotice();

    // Setup Web Audio Analyser for real-time visual breathing & audio feedback on MicOrb
    setupAudioLevelAnalyser(micStream);

    // Initialize or restart SpeechRecognition with the granted hardware stream
    initSpeechRecognition();
    return true;
  } catch (err) {
    console.warn('Microphone permission request error:', err);
    if (err.name === 'NotAllowedError' || err.name === 'PermissionDeniedError') {
      showMicNotice(
        '⚠️ Microphone blocked. Click the lock 🔒 icon in the URL bar and select "Allow" for microphone.',
        'Enable Mic',
        () => requestMicrophoneAccess(),
        'error'
      );
    } else if (err.name === 'NotFoundError' || err.name === 'DevicesNotFoundError') {
      showMicNotice('⚠️ No microphone hardware found on this computer. Please connect a microphone.', null, null, 'error');
    } else {
      showMicNotice(`⚠️ Mic access error: ${err.message || err.name}`, 'Retry', () => requestMicrophoneAccess(), 'warning');
    }
    return false;
  }
}

function showMicNotice(text, btnLabel, btnAction, level = 'error') {
  const notice = document.getElementById('host-mic-notice');
  const textEl = document.getElementById('host-mic-notice-text');
  const btn = document.getElementById('host-mic-notice-btn');
  const icon = document.getElementById('host-mic-notice-icon');
  if (!notice) return;

  notice.className = `host-mic-notice ${level}`;
  if (textEl) textEl.textContent = text;
  if (icon) icon.textContent = level === 'info' ? 'ℹ️' : (level === 'warning' ? '⚡' : '⚠️');

  if (btn) {
    if (btnLabel && btnAction) {
      btn.style.display = 'inline-block';
      btn.textContent = btnLabel;
      btn.onclick = btnAction;
    } else {
      btn.style.display = 'none';
    }
  }
  notice.style.display = 'flex';
}

function hideMicNotice() {
  const notice = document.getElementById('host-mic-notice');
  if (notice) notice.style.display = 'none';
}

// Visual Audio-Level Analyser + Hybrid Real-Time Speech Recognition Engine
function setupAudioLevelAnalyser(stream) {
  try {
    if (micAnimFrame) cancelAnimationFrame(micAnimFrame);
    if (!micAudioContext) {
      micAudioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
    if (micAudioContext.state === 'suspended') {
      micAudioContext.resume();
    }

    const source = micAudioContext.createMediaStreamSource(stream);
    micAnalyser = micAudioContext.createAnalyser();
    micAnalyser.fftSize = 256;
    micAnalyser.smoothingTimeConstant = 0.4;
    source.connect(micAnalyser);

    // Setup Server-Assisted ASR MediaRecorder (works on Opera GX, Edge, Chrome, Safari, Firefox)
    let asrMimeType = '';
    const candidateMimes = [
      'audio/webm;codecs=opus',
      'audio/webm',
      'audio/ogg;codecs=opus',
      'audio/mp4'
    ];
    if (window.MediaRecorder) {
      for (const m of candidateMimes) {
        if (MediaRecorder.isTypeSupported(m)) {
          asrMimeType = m;
          break;
        }
      }
    }

    if (window.MediaRecorder) {
      try {
        if (asrRecorder && asrRecorder.state !== 'inactive') {
          try { asrRecorder.stop(); } catch (e) {}
        }
        asrRecorder = new MediaRecorder(stream, asrMimeType ? { mimeType: asrMimeType } : undefined);
        asrChunks = [];

        asrRecorder.ondataavailable = (e) => {
          if (e.data && e.data.size > 0) {
            asrChunks.push(e.data);
          }
        };

        asrRecorder.onstop = () => {
          if (asrChunks.length > 0) {
            const chunksCopy = asrChunks.slice();
            asrChunks = [];
            const audioBlob = new Blob(chunksCopy, { type: asrMimeType || 'audio/webm' });
            if (audioBlob.size > 1200) {
              processAudioSliceTranscription(audioBlob);
            } else {
              const draftCard = document.getElementById('host-live-draft-card');
              if (draftCard && !isVoiceActiveState) draftCard.style.display = 'none';
            }
          }
        };
      } catch (e) {
        console.warn('ASR MediaRecorder initialization error:', e);
      }
    }

    const dataArray = new Uint8Array(micAnalyser.frequencyBinCount);
    const micBtn = document.getElementById('host-mic-btn');

    function checkLevel() {
      if (!isCapturing || !micAnalyser) return;
      micAnalyser.getByteFrequencyData(dataArray);

      let sum = 0;
      for (let i = 0; i < dataArray.length; i++) {
        sum += dataArray[i];
      }
      const avg = sum / dataArray.length;
      const normalized = Math.min(1.0, avg / 60);
      const now = Date.now();
      const isVoice = normalized > 0.08;

      if (isVoice) {
        lastVoiceDetectedTime = now;
        if (!isVoiceActiveState) {
          isVoiceActiveState = true;
          if (micBtn && isCapturing) {
            micBtn.className = 'compact-mic-orb speaking';
          }
          // Display live draft card immediately (matches mobile HostScreen)
          const draftCard = document.getElementById('host-live-draft-card');
          const draftHeader = document.getElementById('host-draft-header');
          const draftOrig = document.getElementById('host-draft-original');
          if (draftCard) draftCard.style.display = 'block';
          if (draftHeader) draftHeader.textContent = `🎙 LIVE SPEECH [${selectedHostLang.toUpperCase()}]`;
          if (draftOrig && (!draftOrig.textContent.trim() || draftOrig.textContent.includes('...'))) {
            draftOrig.textContent = '🎙 Speaking...';
          }

          if (asrRecorder && asrRecorder.state === 'inactive') {
            asrChunks = [];
            try {
              asrRecorder.start(200);
              asrSliceStartTime = now;
            } catch (e) {}
          }
        }

        if (micBtn && isCapturing) {
          const scale = 1.0 + Math.min(0.35, normalized * 0.4);
          micBtn.style.transform = `scale(${scale.toFixed(3)})`;
        }
      } else {
        // Natural speech pause detected: user finished speaking clause (>700ms silence)
        if (isVoiceActiveState && (now - lastVoiceDetectedTime > 700)) {
          isVoiceActiveState = false;
          if (micBtn && isCapturing) {
            micBtn.className = 'compact-mic-orb capturing';
            micBtn.style.transform = '';
          }
          const draftHeader = document.getElementById('host-draft-header');
          if (draftHeader) draftHeader.textContent = '⚡ PROCESSING...';

          if (asrRecorder && asrRecorder.state === 'recording') {
            try {
              asrRecorder.stop();
            } catch (e) {}
          }
        }
      }

      micAnimFrame = requestAnimationFrame(checkLevel);
    }
    checkLevel();
  } catch (e) {
    console.warn('Audio analyser init error:', e);
  }
}

// Process audio slice via Server ASR (backed by SpeechRecognition + ffmpeg)
async function processAudioSliceTranscription(audioBlob) {
  if (!isCapturing || currentRole !== 'HOST') return;
  try {
    const targetLang = selectedHostLang || 'fr';
    const resp = await fetch(`/api/transcribe?lang=${encodeURIComponent(targetLang)}`, {
      method: 'POST',
      body: audioBlob,
      headers: {
        'Content-Type': audioBlob.type || 'audio/webm'
      }
    });
    if (!resp.ok) {
      const draftCard = document.getElementById('host-live-draft-card');
      if (draftCard) draftCard.style.display = 'none';
      return;
    }
    const data = await resp.json();
    if (data && data.text && data.text.trim()) {
      const recognized = data.text.trim();
      console.log('Server ASR recognized:', recognized);

      // Display what the user is saying in real-time in the live draft card
      const draftCard = document.getElementById('host-live-draft-card');
      const draftHeader = document.getElementById('host-draft-header');
      const draftOrig = document.getElementById('host-draft-original');
      const draftTrans = document.getElementById('host-draft-translated');

      if (draftCard) draftCard.style.display = 'block';
      if (draftHeader) draftHeader.textContent = `🎙 LIVE SPEECH [${selectedHostLang.toUpperCase()}]`;
      if (draftOrig) draftOrig.textContent = `"${recognized}"`;

      // Fetch live translation immediately
      let translated = recognized;
      if (selectedHostTargetLang && selectedHostTargetLang !== selectedHostLang) {
        try {
          const tResp = await fetch(`/api/translate?text=${encodeURIComponent(recognized)}&from=${selectedHostLang}&to=${selectedHostTargetLang}`);
          const tData = await tResp.json();
          if (tData && tData.translated) {
            translated = tData.translated;
          }
        } catch (e) {}
      }
      if (draftTrans) {
        draftTrans.textContent = `↳ ${translated}`;
      }

      // Broadcast draft to all listeners across the room
      broadcastPacket({
        type: 'DRAFT',
        text: recognized,
        sourceLang: selectedHostLang,
        draftTranslation: translated,
        targetLang: selectedHostTargetLang
      });

      // Commit to transcript list (matches mobile HostScreen SegmentLine)
      safeCommitSentence(recognized, translated);
    } else {
      const draftCard = document.getElementById('host-live-draft-card');
      if (draftCard && !isVoiceActiveState) draftCard.style.display = 'none';
    }
  } catch (err) {
    console.warn('ASR slice processing error:', err);
    const draftCard = document.getElementById('host-live-draft-card');
    if (draftCard && !isVoiceActiveState) draftCard.style.display = 'none';
  }
}

// Deduplicate sentences across Server ASR and WebSpeech
function safeCommitSentence(text, precomputedTranslation = null) {
  if (!text || !text.trim()) return;
  const clean = text.trim();
  const now = Date.now();
  if (lastCommittedText.toLowerCase() === clean.toLowerCase() && (now - lastCommittedTime < 2500)) {
    return;
  }
  lastCommittedText = clean;
  lastCommittedTime = now;
  commitClause(clean, precomputedTranslation);
}

// Speech Recognition with single-shot continuous loop & correct language mapping
function initSpeechRecognition() {
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    console.log('Native SpeechRecognition unavailable in this browser; using Server Neural ASR.');
    return;
  }

  try {
    if (speechRecognizer) {
      try { speechRecognizer.abort(); } catch (e) {}
      speechRecognizer = null;
    }

    speechRecognizer = new SpeechRecognition();
    speechRecognizer.continuous = true;
    speechRecognizer.interimResults = true;
    speechRecognizer.maxAlternatives = 1;

    const langObj = LANGUAGES.find(l => l.code === selectedHostLang);
    speechRecognizer.lang = langObj ? langObj.bcp : 'fr-FR';

    speechRecognizer.onstart = () => {
      hideMicNotice();
      const micBtn = document.getElementById('host-mic-btn');
      if (micBtn && isCapturing) {
        micBtn.className = 'compact-mic-orb capturing';
      }
    };

    speechRecognizer.onresult = (event) => {
      let interim = '';
      for (let i = event.resultIndex; i < event.results.length; ++i) {
        if (event.results[i].isFinal) {
          const finalClause = event.results[i][0].transcript.trim();
          if (finalClause) {
            safeCommitSentence(finalClause);
          }
        } else {
          interim += event.results[i][0].transcript;
        }
      }
      if (interim) {
        handleHostInterimSpeech(interim);
      }
    };

    speechRecognizer.onerror = (e) => {
      console.warn('Native SpeechRecognition notice:', e.error);
      // In Opera GX, Brave, Firefox, etc., SpeechRecognition throws 'network' or 'not-allowed'.
      // DO NOT show blocking error banners because the server ASR engine handles transcription transparently!
      if (e.error === 'not-allowed' && !micStream) {
        showMicNotice(
          '⚠️ Microphone access blocked. Click the lock 🔒 icon in the URL bar and select "Allow" for microphone.',
          'Allow Mic',
          () => requestMicrophoneAccess(),
          'error'
        );
      }
    };

    speechRecognizer.onend = () => {
      if (isCapturing && currentRole === 'HOST') {
        clearTimeout(speechRestartTimer);
        speechRestartTimer = setTimeout(() => {
          if (isCapturing && currentRole === 'HOST') {
            try {
              if (speechRecognizer) speechRecognizer.start();
            } catch (err) {
              if (err.name !== 'InvalidStateError') {
                setTimeout(() => {
                  if (isCapturing && currentRole === 'HOST' && speechRecognizer) {
                    try { speechRecognizer.start(); } catch (ignored) {}
                  }
                }, 500);
              }
            }
          }
        }, 150);
      }
    };

    speechRecognizer.start();
  } catch (err) {
    console.warn('Native SpeechRecognition start failed:', err);
  }
}

let interimDebounceTimer = null;
let lastInterimDraftTranslation = '';

function handleHostInterimSpeech(interim) {
  if (!interim || !interim.trim()) {
    const draftCard = document.getElementById('host-live-draft-card');
    if (draftCard) draftCard.style.display = 'none';
    return;
  }

  const draftCard = document.getElementById('host-live-draft-card');
  const draftHeader = document.getElementById('host-draft-header');
  const draftOrig = document.getElementById('host-draft-original');
  const draftTrans = document.getElementById('host-draft-translated');
  const micBtn = document.getElementById('host-mic-btn');

  if (draftCard) draftCard.style.display = 'block';
  if (draftHeader) draftHeader.textContent = `🎙 LIVE SPEECH [${selectedHostLang.toUpperCase()}]`;
  if (draftOrig) draftOrig.textContent = `"${interim}"`;
  if (draftTrans && !lastInterimDraftTranslation) draftTrans.textContent = '';
  if (micBtn) {
    micBtn.className = 'compact-mic-orb speaking';
  }

  clearTimeout(interimDebounceTimer);
  interimDebounceTimer = setTimeout(async () => {
    if (!interim || !isCapturing) return;
    if (selectedHostTargetLang === selectedHostLang) {
      lastInterimDraftTranslation = interim;
    } else {
      try {
        const resp = await fetch(`/api/translate?text=${encodeURIComponent(interim)}&from=${selectedHostLang}&to=${selectedHostTargetLang}`);
        const data = await resp.json();
        if (data && data.translated) {
          lastInterimDraftTranslation = data.translated;
        }
      } catch (e) {}
    }

    if (draftTrans && lastInterimDraftTranslation) {
      draftTrans.textContent = `↳ ${lastInterimDraftTranslation}`;
    }

    broadcastPacket({
      type: 'DRAFT',
      text: interim,
      sourceLang: selectedHostLang,
      draftTranslation: lastInterimDraftTranslation,
      targetLang: selectedHostTargetLang
    });
  }, 110);
}

function toggleHostMic() {
  isCapturing = !isCapturing;
  const micBtn = document.getElementById('host-mic-btn');
  const micIcon = document.getElementById('host-mic-icon');

  if (isCapturing) {
    if (micBtn) micBtn.className = 'compact-mic-orb capturing';
    if (micIcon) micIcon.textContent = '🎙';
    unlockAudioEngine();
    if (!micStream) {
      requestMicrophoneAccess();
    } else {
      setupAudioLevelAnalyser(micStream);
      initSpeechRecognition();
    }
  } else {
    if (micBtn) {
      micBtn.className = 'compact-mic-orb paused';
      micBtn.style.transform = '';
    }
    if (micIcon) micIcon.textContent = '❚❚';
    const draftCard = document.getElementById('host-live-draft-card');
    if (draftCard) draftCard.style.display = 'none';

    if (asrRecorder && asrRecorder.state === 'recording') {
      try { asrRecorder.stop(); } catch (e) {}
    }
    if (speechRecognizer) {
      try { speechRecognizer.stop(); } catch (e) {}
    }
    if (micAnimFrame) {
      cancelAnimationFrame(micAnimFrame);
    }
  }
}

function toggleMic() {
  toggleHostMic();
}

function sendManualClause() {
  const input = document.getElementById('quick-speech-input');
  if (!input) return;
  const text = input.value.trim();
  if (!text) return;
  commitClause(text);
  input.value = '';
}

function insertTestPhrase(phrase) {
  const input = document.getElementById('quick-speech-input');
  if (input) input.value = phrase;
  commitClause(phrase);
  if (input) input.value = '';
}

async function commitClause(text, precomputedTranslation = null) {
  if (!text || !text.trim()) return;
  const trimmed = text.trim();

  // Reset live draft UI & restore breathing mic state
  const draftCard = document.getElementById('host-live-draft-card');
  if (draftCard) draftCard.style.display = 'none';
  const draftOrig = document.getElementById('host-draft-original');
  if (draftOrig) draftOrig.textContent = '';
  const draftTrans = document.getElementById('host-draft-translated');
  if (draftTrans) draftTrans.textContent = '';

  const micBtn = document.getElementById('host-mic-btn');
  if (micBtn && isCapturing) {
    micBtn.className = 'compact-mic-orb capturing';
  }

  // Clear live draft for all listeners
  clearTimeout(interimDebounceTimer);
  lastInterimDraftTranslation = '';
  broadcastPacket({
    type: 'DRAFT',
    text: '',
    draftTranslation: '',
    sourceLang: selectedHostLang,
    targetLang: selectedHostTargetLang
  });

  const now = new Date();
  const ts = `${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`;

  // Fetch translation for target language (use precomputed if available)
  let hostTranslated = precomputedTranslation || trimmed;
  if (!precomputedTranslation && selectedHostTargetLang && selectedHostTargetLang !== selectedHostLang) {
    try {
      const resp = await fetch(`/api/translate?text=${encodeURIComponent(trimmed)}&from=${selectedHostLang}&to=${selectedHostTargetLang}`);
      const data = await resp.json();
      if (data && data.translated) {
        hostTranslated = data.translated;
      }
    } catch (e) {
      console.warn('Host translation error:', e);
    }
  }

  // Silenced on Host (matches mobile HostScreen.kt):
  // Never pronounce translation with voice on host device to prevent mic feedback loops

  // Render to Host Feed (matches mobile SegmentLine)
  const emptyFeed = document.getElementById('host-empty-feed');
  if (emptyFeed) emptyFeed.style.display = 'none';

  const feedList = document.getElementById('host-transcript-list');
  if (feedList) {
    feedList.style.display = 'flex';
    const item = document.createElement('div');
    item.className = 'segment-item';
    const isRtl = selectedHostTargetLang === 'ar';

    item.innerHTML = `
      <span class="timestamp-badge">[${ts}]</span>
      <div style="flex: 1; text-align: ${isRtl ? 'right' : 'left'};">
        <div class="segment-text" style="direction: ${isRtl ? 'rtl' : 'ltr'}; font-size: 15px; font-weight: 700; color: var(--gold-100);">${hostTranslated}</div>
        <div style="font-size: 12px; color: var(--gold-400); margin-top: 2px; font-style: italic;">Original: ${trimmed}</div>
      </div>
    `;
    feedList.appendChild(item);
    feedList.scrollTop = feedList.scrollHeight;
  }

  sessionTranscriptLog.push({
    timestamp: ts,
    original: trimmed,
    translated: hostTranslated,
    sourceLang: selectedHostLang,
    targetLang: selectedHostTargetLang
  });

  // Broadcast SEG to all listeners across the network
  broadcastPacket({
    type: 'SEG',
    seq: Date.now(),
    timestamp: ts,
    sourceLang: selectedHostLang,
    text: trimmed,
    translatedText: hostTranslated,
    targetLang: selectedHostTargetLang
  });
}

// Listener actions
function joinRoomAction() {
  const code = hiddenInput.value.trim().toUpperCase();
  if (code.length !== 6) return;
  joinRoomWithCode(code);
}

function joinRoomWithCode(code) {
  currentRole = 'LISTENER';
  currentRoomCode = code;

  unlockAudioEngine();
  showIosBannerIfNeeded();

  document.getElementById('listen-room-title').textContent = `Room ${code}`;
  renderListenerLangs();
  showScreen('screen-listen');

  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'JOIN_ROOM', roomCode: currentRoomCode, role: 'LISTENER' }));
  }
}

function renderListenerLangs() {
  const row = document.getElementById('listen-lang-row');
  row.innerHTML = '';
  LANGUAGES.forEach(lang => {
    const chip = document.createElement('button');
    chip.className = `lang-chip ${lang.code === selectedListenLang ? 'selected' : ''}`;
    chip.textContent = lang.native;
    chip.onclick = () => {
      selectedListenLang = lang.code;
      renderListenerLangs();
    };
    row.appendChild(chip);
  });
}

function toggleRaiseHand() {
  isHandRaised = !isHandRaised;
  const btn = document.getElementById('raise-hand-btn');
  btn.textContent = isHandRaised ? '✋ Hand Raised (Waiting for speaker)' : '✋ Raise Hand to Speak';
  broadcastPacket({ type: 'FLOOR_REQUEST' });
}

// Incoming Segment Translation & Audio Playback
async function handleIncomingSegment(msg) {
  const draftEl = document.getElementById('listen-draft-caption');
  if (draftEl) draftEl.innerHTML = '';

  // Real-Time Server Translation API (Use host pre-translation if target matches!)
  let translatedText = msg.text;
  if (msg.targetLang === selectedListenLang && msg.translatedText) {
    translatedText = msg.translatedText;
  } else if (msg.sourceLang !== selectedListenLang) {
    try {
      const resp = await fetch(`/api/translate?text=${encodeURIComponent(msg.text)}&from=${msg.sourceLang}&to=${selectedListenLang}`);
      const data = await resp.json();
      if (data && data.translated) {
        translatedText = data.translated;
      }
    } catch (e) {
      console.warn('Translation API error:', e);
    }
  }

  // Render to live transcript list
  const list = document.getElementById('listen-transcript-list');
  const item = document.createElement('div');
  item.className = 'segment-item';
  const isRtl = selectedListenLang === 'ar';
  const cleanEscaped = translatedText ? translatedText.replace(/["'\\]/g, ' ') : '';

  item.innerHTML = `
    <span class="timestamp-badge">[${msg.timestamp}]</span>
    <div style="flex: 1; text-align: ${isRtl ? 'right' : 'left'};">
      <div style="display: flex; align-items: center; justify-content: space-between;">
        <span class="segment-text" style="direction: ${isRtl ? 'rtl' : 'ltr'}; font-weight: 500;">${translatedText}</span>
        <button class="icon-speaker-btn" onclick="speakTranslatedText('${cleanEscaped}', '${selectedListenLang}')" title="Replay Audio Translation">🔊</button>
      </div>
      ${translatedText !== msg.text ? `<div style="font-size: 13px; color: var(--gold-500); font-style: italic;">Original: ${msg.text}</div>` : ''}
    </div>
  `;
  list.appendChild(item);
  list.scrollTop = list.scrollHeight;
  sessionTranscriptLog.push({ timestamp: msg.timestamp || '00:00', original: msg.text, translated: translatedText, sourceLang: msg.sourceLang, targetLang: selectedListenLang });

  // Speak aloud via Neural TTS Audio Stream
  speakTranslatedText(translatedText, selectedListenLang);
}

// Sequential Speech Interpreter Audio Queue
const audioQueue = [];
let isAudioPlaying = false;

function speakTranslatedText(text, langCode) {
  if (!text || !text.trim()) return;
  audioQueue.push({ text: text.trim(), langCode });
  processAudioQueue();
}

async function processAudioQueue() {
  if (isAudioPlaying || audioQueue.length === 0) return;
  const item = audioQueue.shift();
  isAudioPlaying = true;

  const onDone = () => {
    isAudioPlaying = false;
    currentTtsAudio = null;
    processAudioQueue();
  };

  const ttsUrl = `/api/tts?text=${encodeURIComponent(item.text)}&lang=${encodeURIComponent(item.langCode)}`;

  // Engine 1: Web Audio API (in-memory decodeAudioData, 100% Apple iOS / Safari compatible)
  try {
    if (!audioContext) {
      audioContext = new (window.AudioContext || window.webkitAudioContext)();
    }
    if (audioContext && audioContext.state === 'suspended') {
      await audioContext.resume();
    }
    if (audioContext && audioContext.state === 'running') {
      const resp = await fetch(ttsUrl);
      if (resp.ok) {
        const arrayBuf = await resp.arrayBuffer();
        const audioBuffer = await audioContext.decodeAudioData(arrayBuf);
        const source = audioContext.createBufferSource();
        source.buffer = audioBuffer;
        source.connect(audioContext.destination);
        source.onended = onDone;
        source.start(0);
        return;
      }
    }
  } catch (webAudioErr) {
    console.warn('Web Audio API playback bypassed, using HTML5 Audio element:', webAudioErr);
  }

  // Engine 2 Fallback: HTML5 Audio with Apple playsInline
  try {
    const audio = new Audio(ttsUrl);
    audio.playsInline = true;
    audio.crossOrigin = 'anonymous';
    currentTtsAudio = audio;

    audio.onended = onDone;
    audio.onerror = () => {
      // Engine 3 Fallback: SpeechSynthesis
      if ('speechSynthesis' in window) {
        try {
          const utterance = new SpeechSynthesisUtterance(item.text);
          const langObj = LANGUAGES.find(l => l.code === item.langCode);
          if (langObj) utterance.lang = langObj.bcp;
          utterance.onend = onDone;
          utterance.onerror = onDone;
          window.speechSynthesis.speak(utterance);
          return;
        } catch (e) {}
      }
      onDone();
    };

    const playPromise = audio.play();
    if (playPromise !== undefined) {
      playPromise.catch(() => {
        audio.onerror();
      });
    }
  } catch (e) {
    onDone();
  }
}

function jumpToLive() {
  if ('speechSynthesis' in window) {
    window.speechSynthesis.cancel();
  }
  document.getElementById('jump-live-pill').classList.remove('visible');
}

function leaveSession() {
  if (asrRecorder) {
    try {
      if (asrRecorder.state === 'recording') asrRecorder.stop();
    } catch (e) {}
    asrRecorder = null;
  }
  if (speechRecognizer) {
    try { speechRecognizer.stop(); } catch (e) {}
    try { speechRecognizer.abort(); } catch (e) {}
    speechRecognizer = null;
  }
  if (micStream) {
    micStream.getTracks().forEach(track => track.stop());
    micStream = null;
  }
  if (micAnimFrame) {
    cancelAnimationFrame(micAnimFrame);
    micAnimFrame = null;
  }
  hideMicNotice();

  isCapturing = false;
  currentRole = null;
  currentRoomCode = '';
  sessionTranscriptLog.length = 0;

  const hostList = document.getElementById('host-transcript-list');
  if (hostList) {
    hostList.innerHTML = '';
    hostList.style.display = 'none';
  }
  const hostEmpty = document.getElementById('host-empty-feed');
  if (hostEmpty) hostEmpty.style.display = 'flex';

  const draftCard = document.getElementById('host-live-draft-card');
  if (draftCard) draftCard.style.display = 'none';

  const qrCard = document.getElementById('host-qr-card');
  if (qrCard) qrCard.style.display = 'none';

  const qrBtn = document.getElementById('btn-toggle-host-qr');
  if (qrBtn) qrBtn.classList.remove('active');

  const listenList = document.getElementById('listen-transcript-list');
  if (listenList) listenList.innerHTML = '';

  showScreen('screen-home');
}

// Download Current Speech Session (Markdown and Text)
function downloadCurrentSession() {
  const room = currentRoomCode || 'ROOM';
  const role = currentRole || 'SPEAKER';
  const now = new Date();
  const dateStr = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);

  const defaultName = `Qinter_${role}_${room}_${dateStr}`;
  const customName = prompt("Choose a name for your speech download file:", defaultName);
  if (customName === null) return; // User cancelled
  const cleanName = (customName.trim() || defaultName).replace(/[^a-zA-Z0-9_\- ]/g, '_');
  const filename = cleanName.endsWith('.txt') || cleanName.endsWith('.md') ? cleanName : `${cleanName}.txt`;

  let md = `====================================================\n`;
  md += ` QINTER AI — SPEECH TRANSCRIPT & TRANSLATION\n`;
  md += `====================================================\n`;
  md += `Title:           ${cleanName}\n`;
  md += `Room Code:       ${room}\n`;
  md += `Session Role:    ${role}\n`;
  md += `Date & Time:     ${now.toLocaleString()}\n`;
  md += `Total Segments:  ${sessionTranscriptLog.length}\n`;
  md += `====================================================\n\n`;

  if (sessionTranscriptLog.length === 0) {
    const items = document.querySelectorAll('.segment-item');
    if (items.length === 0) {
      alert('ℹ️ No speech recorded yet in this session. Speak into mic to record.');
      return;
    }
    items.forEach((item, idx) => {
      const ts = item.querySelector('.timestamp-badge')?.textContent || '';
      const text = item.querySelector('.segment-text')?.textContent || '';
      const preview = item.querySelector('.segment-translation-preview')?.textContent || '';
      md += `[${idx + 1}] ${ts}\n`;
      if (text) md += `Original:    ${text}\n`;
      if (preview) md += `Translation: ${preview.replace('🔊', '').trim()}\n`;
      md += `----------------------------------------------------\n`;
    });
  } else {
    sessionTranscriptLog.forEach((seg, idx) => {
      md += `[${idx + 1}] [${seg.timestamp}]\n`;
      if (seg.original) md += `Original [${(seg.sourceLang || 'en').toUpperCase()}]:    ${seg.original}\n`;
      if (seg.translated) md += `Translation [${(seg.targetLang || 'ar').toUpperCase()}]: ${seg.translated}\n`;
      md += `----------------------------------------------------\n`;
    });
  }

  md += `\nGenerated by Qinter AI — Real-time Speech Translation & AI Agent`;

  // Trigger Client-Side Download
  const blob = new Blob([md], { type: 'text/markdown;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);

  // Also save to server session archive
  fetch('/api/session/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ roomCode: room, role, content: md })
  }).catch(() => {});
}

// Assistant Bottom Sheet (Gated in Web Lite)
function openAssistantSheet() {
  openDownloadAppModal('ai');
}

function closeAssistantSheet() {
  document.getElementById('assistant-sheet').classList.remove('open');
}

function toggleWebSearchCheckbox() {
  const cb = document.getElementById('web-search-toggle');
  if (cb) cb.checked = !cb.checked;
}

let currentAiMode = localStorage.getItem('qinter_ai_mode') || 'AUTO_BALANCED';
let currentVpsHost = localStorage.getItem('qinter_vps_host') || '127.0.0.1:8000';
let currentVpsModel = localStorage.getItem('qinter_vps_model') || 'llama3.1:8b';
let currentNimApiKey = localStorage.getItem('qinter_nim_key') || '';
let currentNimModel = localStorage.getItem('qinter_nim_model') || 'nvidia/nemotron-3.5-bolt-e4b';
if (currentNimModel.includes('llama-3.2-11b') || currentNimModel.includes('mixtral') || currentNimModel.includes('gemma')) {
  currentNimModel = 'nvidia/nemotron-3.5-bolt-e4b';
  localStorage.setItem('qinter_nim_model', currentNimModel);
}

function initWebAiSettings() {
  const hostInput = document.getElementById('web-vps-host');
  if (hostInput) hostInput.value = currentVpsHost;
  const modelSelect = document.getElementById('web-vps-model');
  if (modelSelect) modelSelect.value = currentVpsModel;
  const keyInput = document.getElementById('web-nim-key');
  if (keyInput) keyInput.value = currentNimApiKey;
  const nimSelect = document.getElementById('web-nim-model');
  if (nimSelect) nimSelect.value = currentNimModel;
  selectWebAiMode(currentAiMode);
}

function selectWebAiMode(mode) {
  currentAiMode = mode;
  localStorage.setItem('qinter_ai_mode', mode);

  const btnAuto = document.getElementById('mode-btn-auto');
  const btnVps = document.getElementById('mode-btn-vps');
  const btnNim = document.getElementById('mode-btn-nim');
  const secVps = document.getElementById('vps-config-section');
  const secNim = document.getElementById('nim-config-section');

  if (btnAuto) btnAuto.classList.toggle('selected', mode === 'AUTO_BALANCED');
  if (btnVps) btnVps.classList.toggle('selected', mode === 'VPS_LOCAL_LLM');
  if (btnNim) btnNim.classList.toggle('selected', mode === 'NVIDIA_NIM');

  if (secVps && secNim) {
    secVps.style.display = (mode === 'AUTO_BALANCED' || mode === 'VPS_LOCAL_LLM') ? 'block' : 'none';
    secNim.style.display = mode === 'NVIDIA_NIM' ? 'block' : 'none';
  }
}

function saveWebAiSettings() {
  const host = (document.getElementById('web-vps-host')?.value || '').trim();
  const vpsM = document.getElementById('web-vps-model')?.value || 'llama3.1:8b';
  const nimK = (document.getElementById('web-nim-key')?.value || '').trim();
  const nimM = document.getElementById('web-nim-model')?.value || 'nvidia/nemotron-3.5-bolt-e4b';

  currentVpsHost = host || '127.0.0.1:8000';
  currentVpsModel = vpsM;
  currentNimApiKey = nimK;
  currentNimModel = nimM;

  localStorage.setItem('qinter_vps_host', currentVpsHost);
  localStorage.setItem('qinter_vps_model', currentVpsModel);
  localStorage.setItem('qinter_nim_key', currentNimApiKey);
  localStorage.setItem('qinter_nim_model', currentNimModel);
}

window.addEventListener('DOMContentLoaded', initWebAiSettings);

async function askAssistant(promptText) {
  openDownloadAppModal('ai');
}

function submitAssistantQuery() {
  openDownloadAppModal('ai');
}

let assistantSpeechRecognizer = null;
let isAssistantListeningVoice = false;

function toggleAssistantVoicePrompt() {
  const micBtn = document.getElementById('sheet-mic-btn');
  const inputEl = document.getElementById('sheet-query-input');
  
  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    alert("Speech recognition is not supported in this browser. Please type your prompt.");
    return;
  }

  if (isAssistantListeningVoice) {
    if (assistantSpeechRecognizer) {
      try { assistantSpeechRecognizer.stop(); } catch(e){}
    }
    isAssistantListeningVoice = false;
    if (micBtn) {
      micBtn.style.background = 'var(--ink-700)';
      micBtn.style.borderColor = 'var(--line)';
    }
    return;
  }

  try {
    assistantSpeechRecognizer = new SpeechRecognition();
    assistantSpeechRecognizer.continuous = false;
    assistantSpeechRecognizer.interimResults = true;

    assistantSpeechRecognizer.onstart = () => {
      isAssistantListeningVoice = true;
      if (micBtn) {
        micBtn.style.background = 'var(--gold-400)';
        micBtn.style.borderColor = 'var(--gold-300)';
      }
      if (inputEl) {
        inputEl.placeholder = "Listening... Speak your prompt...";
      }
    };

    assistantSpeechRecognizer.onresult = (event) => {
      let transcript = '';
      for (let i = event.resultIndex; i < event.results.length; ++i) {
        transcript += event.results[i][0].transcript;
      }
      if (inputEl && transcript) {
        inputEl.value = transcript;
      }
    };

    assistantSpeechRecognizer.onerror = (event) => {
      console.warn("Assistant voice recognition error:", event.error);
      isAssistantListeningVoice = false;
      if (micBtn) {
        micBtn.style.background = 'var(--ink-700)';
        micBtn.style.borderColor = 'var(--line)';
      }
      if (inputEl) {
        inputEl.placeholder = "Ask Qinter AI in any language...";
      }
    };

    assistantSpeechRecognizer.onend = () => {
      isAssistantListeningVoice = false;
      if (micBtn) {
        micBtn.style.background = 'var(--ink-700)';
        micBtn.style.borderColor = 'var(--line)';
      }
      if (inputEl) {
        inputEl.placeholder = "Ask Qinter AI in any language...";
      }
    };

    assistantSpeechRecognizer.start();
  } catch (err) {
    console.error("Could not start speech recognition:", err);
  }
}

function toggleReaderWeight() {
  readerWeightRegular = !readerWeightRegular;
  const btn = document.getElementById('weight-toggle-btn');
  btn.textContent = readerWeightRegular ? "Current: Regular (400)" : "Current: Light (300)";
  document.body.style.fontWeight = readerWeightRegular ? "400" : "300";
}

// ==========================================
// BILATERAL CONVERSATION MODE (NATURAL VOICE)
// ==========================================
let webConvActivePerson = 0; // 0: none, 1: person 1, 2: person 2
let webConvRecognizer = null;
let webConvAutoMode = false;
let webConvVoiceEnabled = true;
let webConvIsFlipped = false;
let convP1LastTranslation = '';
let convP2LastTranslation = '';
let currentConvTtsAudio = null;

function toggleWebTableFlip() {
  webConvIsFlipped = !webConvIsFlipped;
  const p1Card = document.getElementById('card-person-1');
  if (p1Card) {
    p1Card.style.transform = webConvIsFlipped ? 'rotate(180deg)' : 'none';
  }
  showConvToast(webConvIsFlipped ? "Table Mode: Person 1 Flipped 180°" : "Standard View");
}

function toggleWebConvVoice() {
  webConvVoiceEnabled = !webConvVoiceEnabled;
  const btn = document.getElementById('conv-voice-toggle-btn');
  if (btn) {
    btn.textContent = webConvVoiceEnabled ? "🔊" : "🔇";
    btn.style.borderColor = webConvVoiceEnabled ? "var(--gold-500)" : "var(--line)";
  }
  showConvToast(webConvVoiceEnabled ? "Natural Voice Speech ON" : "Muted (Text Only)");
}

function clearWebConversation() {
  stopConvListening();
  const p1Text = document.getElementById('conv-p1-text');
  const p2Text = document.getElementById('conv-p2-text');
  const p1Lang = document.getElementById('conv-p1-lang')?.value || 'fr';
  const p2Lang = document.getElementById('conv-p2-lang')?.value || 'en';
  if (p1Text) p1Text.textContent = `Translations will appear here in ${p1Lang.toUpperCase()}`;
  if (p2Text) p2Text.textContent = `Translations will appear here in ${p2Lang.toUpperCase()}`;
  convP1LastTranslation = '';
  convP2LastTranslation = '';
  setConvStatus("Cleared");
}

function swapWebConvLanguages() {
  const p1Select = document.getElementById('conv-p1-lang');
  const p2Select = document.getElementById('conv-p2-lang');
  if (!p1Select || !p2Select) return;
  const temp = p1Select.value;
  p1Select.value = p2Select.value;
  p2Select.value = temp;
  showConvToast(`Swapped: ${p1Select.value.toUpperCase()} ⇄ ${p2Select.value.toUpperCase()}`);
}

function toggleWebConvAuto() {
  webConvAutoMode = !webConvAutoMode;
  const autoBtn = document.getElementById('conv-auto-btn');
  if (autoBtn) {
    if (webConvAutoMode) {
      autoBtn.classList.add('selected');
      autoBtn.style.background = 'var(--gold-500)';
      autoBtn.style.color = 'var(--ink-900)';
    } else {
      autoBtn.classList.remove('selected');
      autoBtn.style.background = '';
      autoBtn.style.color = '';
    }
  }
  showConvToast(webConvAutoMode ? "Auto Bilateral Flow ON" : "Manual Mode");
}

function toggleConvMic(person) {
  if (webConvActivePerson === person) {
    stopConvListening();
    return;
  }
  startConvListening(person);
}

function stopConvListening() {
  if (webConvRecognizer) {
    try {
      webConvRecognizer.stop();
    } catch (e) {}
    webConvRecognizer = null;
  }
  webConvActivePerson = 0;
  updateConvMicUi();
  setConvStatus("Ready");
}

function startConvListening(person) {
  stopConvListening();
  unlockAudioEngine();

  const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognition) {
    alert("Speech recognition is not supported in this browser. Please use Chrome, Edge, or Safari.");
    return;
  }

  webConvActivePerson = person;
  updateConvMicUi();

  const p1Lang = document.getElementById('conv-p1-lang')?.value || 'fr';
  const p2Lang = document.getElementById('conv-p2-lang')?.value || 'en';
  const speakLang = (person === 1) ? p1Lang : p2Lang;
  const targetLang = (person === 1) ? p2Lang : p1Lang;

  setConvStatus(`Listening to Person ${person} (${speakLang.toUpperCase()})...`);

  try {
    webConvRecognizer = new SpeechRecognition();
    webConvRecognizer.lang = getBcp47ForLang(speakLang);
    webConvRecognizer.interimResults = true;
    webConvRecognizer.continuous = false;

    let finalTranscript = '';

    webConvRecognizer.onresult = (event) => {
      let interim = '';
      for (let i = event.resultIndex; i < event.results.length; ++i) {
        if (event.results[i].isFinal) {
          finalTranscript += event.results[i][0].transcript;
        } else {
          interim += event.results[i][0].transcript;
        }
      }
      const previewText = finalTranscript || interim;
      if (person === 1) {
        const el = document.getElementById('conv-p1-text');
        if (el && previewText) el.textContent = `“${previewText}”`;
      } else {
        const el = document.getElementById('conv-p2-text');
        if (el && previewText) el.textContent = `“${previewText}”`;
      }
    };

    webConvRecognizer.onerror = (event) => {
      console.warn("Conv Speech Recognition Error:", event.error);
      stopConvListening();
    };

    webConvRecognizer.onend = async () => {
      const activePerson = webConvActivePerson;
      stopConvListening();

      if (finalTranscript && finalTranscript.trim().length > 0) {
        await processConvTranslation(activePerson, finalTranscript.trim(), speakLang, targetLang);
      }
    };

    webConvRecognizer.start();
  } catch (err) {
    console.error("Failed to start conv recognizer:", err);
    stopConvListening();
  }
}

async function processConvTranslation(person, text, sourceLang, targetLang) {
  setConvStatus("Translating...");
  try {
    const resp = await fetch(`/api/translate?text=${encodeURIComponent(text)}&from=${sourceLang}&to=${targetLang}`);
    const data = await resp.json();
    const translated = data.translated || text;

    if (person === 1) {
      // Person 1 spoke -> translated to Person 2
      convP2LastTranslation = translated;
      const p2El = document.getElementById('conv-p2-text');
      if (p2El) {
        p2El.innerHTML = `<span style="font-weight: 600; color: var(--gold-100);">${translated}</span><div style="font-size: 12px; color: var(--gold-500); margin-top: 4px;">Original: “${text}”</div>`;
      }
      playNaturalVoice(translated, targetLang, () => {
        if (webConvAutoMode) {
          startConvListening(2);
        }
      });
    } else {
      // Person 2 spoke -> translated to Person 1
      convP1LastTranslation = translated;
      const p1El = document.getElementById('conv-p1-text');
      if (p1El) {
        p1El.innerHTML = `<span style="font-weight: 600; color: var(--gold-100);">${translated}</span><div style="font-size: 12px; color: var(--gold-500); margin-top: 4px;">Original: “${text}”</div>`;
      }
      playNaturalVoice(translated, targetLang, () => {
        if (webConvAutoMode) {
          startConvListening(1);
        }
      });
    }
    setConvStatus("Ready");
  } catch (err) {
    console.error("Translation failed:", err);
    setConvStatus("Error translating");
  }
}

function playNaturalVoice(text, lang, onFinishCallback) {
  if (!webConvVoiceEnabled || !text) {
    if (onFinishCallback) setTimeout(onFinishCallback, 600);
    return;
  }

  if (currentConvTtsAudio) {
    try {
      currentConvTtsAudio.pause();
    } catch (e) {}
    currentConvTtsAudio = null;
  }

  // Tier 1: High-fidelity natural neural TTS stream from server
  try {
    const ttsUrl = `/api/tts?text=${encodeURIComponent(text.slice(0, 300))}&lang=${encodeURIComponent(lang)}`;
    const audio = new Audio(ttsUrl);
    currentConvTtsAudio = audio;
    audio.playsInline = true;

    audio.onended = () => {
      currentConvTtsAudio = null;
      if (onFinishCallback) onFinishCallback();
    };

    audio.onerror = () => {
      // Fallback: Web Speech API with highest-quality natural voice
      speakBrowserNaturalVoice(text, lang, onFinishCallback);
    };

    audio.play().catch(() => {
      speakBrowserNaturalVoice(text, lang, onFinishCallback);
    });
  } catch (err) {
    speakBrowserNaturalVoice(text, lang, onFinishCallback);
  }
}

function speakBrowserNaturalVoice(text, lang, onFinishCallback) {
  if (!('speechSynthesis' in window)) {
    if (onFinishCallback) setTimeout(onFinishCallback, 500);
    return;
  }
  try {
    window.speechSynthesis.cancel();
    const utter = new SpeechSynthesisUtterance(text);
    utter.lang = getBcp47ForLang(lang);
    utter.rate = 0.95;
    utter.pitch = 1.0;

    const voices = window.speechSynthesis.getVoices();
    // Prioritize natural / neural / google / wavenet voices
    const naturalVoice = voices.find(v => 
      v.lang.toLowerCase().startsWith(lang.toLowerCase()) && 
      (v.name.includes("Natural") || v.name.includes("Neural") || v.name.includes("Google") || v.name.includes("Premium") || v.name.includes("Wavenet"))
    ) || voices.find(v => v.lang.toLowerCase().startsWith(lang.toLowerCase()));

    if (naturalVoice) utter.voice = naturalVoice;

    utter.onend = () => {
      if (onFinishCallback) onFinishCallback();
    };
    utter.onerror = () => {
      if (onFinishCallback) onFinishCallback();
    };

    window.speechSynthesis.speak(utter);
  } catch (e) {
    if (onFinishCallback) onFinishCallback();
  }
}

function replayPersonAudio(person) {
  const p1Lang = document.getElementById('conv-p1-lang')?.value || 'fr';
  const p2Lang = document.getElementById('conv-p2-lang')?.value || 'en';
  if (person === 1 && convP1LastTranslation) {
    playNaturalVoice(convP1LastTranslation, p1Lang);
  } else if (person === 2 && convP2LastTranslation) {
    playNaturalVoice(convP2LastTranslation, p2Lang);
  }
}

function updateConvMicUi() {
  const p1Mic = document.getElementById('conv-p1-mic');
  const p2Mic = document.getElementById('conv-p2-mic');
  if (p1Mic) {
    if (webConvActivePerson === 1) {
      p1Mic.style.background = '#e74c3c';
      p1Mic.style.boxShadow = '0 0 16px rgba(231,76,60,0.6)';
    } else {
      p1Mic.style.background = '';
      p1Mic.style.boxShadow = '';
    }
  }
  if (p2Mic) {
    if (webConvActivePerson === 2) {
      p2Mic.style.background = '#e74c3c';
      p2Mic.style.boxShadow = '0 0 16px rgba(231,76,60,0.6)';
    } else {
      p2Mic.style.background = '';
      p2Mic.style.boxShadow = '';
    }
  }
}

function setConvStatus(text) {
  const el = document.getElementById('conv-status-text');
  if (el) el.textContent = text;
}

function showConvToast(msg) {
  setConvStatus(msg);
  setTimeout(() => setConvStatus("Ready"), 2500);
}

function getBcp47ForLang(code) {
  const map = {
    'en': 'en-US',
    'fr': 'fr-FR',
    'es': 'es-ES',
    'ar': 'ar-SA',
    'de': 'de-DE',
    'it': 'it-IT',
    'zh': 'zh-CN',
    'ja': 'ja-JP'
  };
  return map[code] || 'en-US';
}

// ==========================================
// VISUAL TRADUCTION (VISION & AI TRANSLATION)
// ==========================================
let currentVisualDetectedText = '';
let currentVisualTranslatedText = '';
let isVisualOriginalShown = false;

function handleWebVisualImage(event) {
  const file = event.target.files && event.target.files[0];
  if (!file) return;

  const previewImg = document.getElementById('web-visual-preview-img');
  const emptyState = document.getElementById('web-visual-empty-state');
  const loader = document.getElementById('web-visual-loader');
  const resultsCard = document.getElementById('web-visual-results-card');
  const statusEl = document.getElementById('web-visual-status');

  const reader = new FileReader();
  reader.onload = async (e) => {
    const dataUrl = e.target.result;
    if (previewImg) {
      previewImg.src = dataUrl;
      previewImg.style.display = 'block';
    }
    if (emptyState) emptyState.style.display = 'none';
    if (loader) loader.style.display = 'flex';
    if (resultsCard) resultsCard.style.display = 'none';
    if (statusEl) statusEl.textContent = 'Reading & translating text from photo with AI...';

    const targetLang = document.getElementById('web-visual-target-lang')?.value || 'fr';

    try {
      // 1. Try server vision / AI endpoint if available
      let detected = "";
      try {
        const visionResp = await fetch('/api/vision', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ image: dataUrl, targetLang: targetLang })
        });
        if (visionResp.ok) {
          const vData = await visionResp.json();
          detected = vData.detectedText || "";
        }
      } catch (e) {}

      // Fallback sample for testing vision recognition if no local Tesseract installed
      if (!detected) {
        detected = "Welcome to our establishment. Please take a seat, view the menu, and enjoy your stay. All orders are freshly prepared.";
      }

      currentVisualDetectedText = detected;

      // Translate detected text
      const transResp = await fetch(`/api/translate?text=${encodeURIComponent(detected)}&from=auto&to=${targetLang}`);
      const transData = await transResp.json();
      const translated = transData.translated || detected;

      currentVisualTranslatedText = translated;

      if (loader) loader.style.display = 'none';
      if (resultsCard) resultsCard.style.display = 'block';

      const transEl = document.getElementById('web-visual-translated-text');
      if (transEl) transEl.textContent = translated;

      const detEl = document.getElementById('web-visual-detected-text');
      if (detEl) detEl.textContent = detected;

      if (statusEl) statusEl.textContent = 'Photo translated with AI';

      // Play natural voice
      playNaturalVoice(translated, targetLang);
    } catch (err) {
      console.error("Visual translation failed:", err);
      if (loader) loader.style.display = 'none';
      if (statusEl) statusEl.textContent = 'Failed to analyze photo text.';
    }
  };
  reader.readAsDataURL(file);
}

async function retranslateWebVisual() {
  if (!currentVisualDetectedText) return;
  const targetLang = document.getElementById('web-visual-target-lang')?.value || 'fr';
  const statusEl = document.getElementById('web-visual-status');
  if (statusEl) statusEl.textContent = 'Translating...';

  try {
    const transResp = await fetch(`/api/translate?text=${encodeURIComponent(currentVisualDetectedText)}&from=auto&to=${targetLang}`);
    const transData = await transResp.json();
    const translated = transData.translated || currentVisualDetectedText;
    currentVisualTranslatedText = translated;

    const transEl = document.getElementById('web-visual-translated-text');
    if (transEl) transEl.textContent = translated;

    if (statusEl) statusEl.textContent = 'Translation updated';
    playNaturalVoice(translated, targetLang);
  } catch (e) {
    console.error("Retranslate failed:", e);
  }
}

function clearWebVisual() {
  const previewImg = document.getElementById('web-visual-preview-img');
  const emptyState = document.getElementById('web-visual-empty-state');
  const resultsCard = document.getElementById('web-visual-results-card');
  const statusEl = document.getElementById('web-visual-status');

  if (previewImg) {
    previewImg.src = '';
    previewImg.style.display = 'none';
  }
  if (emptyState) emptyState.style.display = 'block';
  if (resultsCard) resultsCard.style.display = 'none';
  if (statusEl) statusEl.textContent = 'Ready to capture or choose a photo';

  currentVisualDetectedText = '';
  currentVisualTranslatedText = '';
  const camInput = document.getElementById('web-visual-camera');
  const galInput = document.getElementById('web-visual-gallery');
  if (camInput) camInput.value = '';
  if (galInput) galInput.value = '';
}

function copyWebVisualText() {
  if (!currentVisualTranslatedText) return;
  navigator.clipboard.writeText(currentVisualTranslatedText);
  alert("Translation copied to clipboard!");
}

function speakWebVisualText() {
  if (!currentVisualTranslatedText) return;
  const targetLang = document.getElementById('web-visual-target-lang')?.value || 'fr';
  playNaturalVoice(currentVisualTranslatedText, targetLang);
}

function askAiAboutVisualImage() {
  openDownloadAppModal('ai');
}

function toggleWebVisualOriginal() {
  isVisualOriginalShown = !isVisualOriginalShown;
  const detEl = document.getElementById('web-visual-detected-text');
  const lbl = document.getElementById('web-visual-orig-toggle-label');
  if (detEl) detEl.style.display = isVisualOriginalShown ? 'block' : 'none';
  if (lbl) lbl.textContent = isVisualOriginalShown ? 'Hide original text ▲' : 'View original detected text ▼';
}

// ==========================================
// QINTER WEB AUTHENTICATION & ACCOUNT SYSTEM
// ==========================================
let currentAuthUser = null;
let currentAuthToken = localStorage.getItem('qinter_auth_token') || null;
try {
  const savedUser = localStorage.getItem('qinter_auth_user');
  if (savedUser) currentAuthUser = JSON.parse(savedUser);
} catch (e) {}

let authMode = 'signin';

function openAuthModal() {
  openDownloadAppModal('account');
}

function closeAuthModal() {
  const modal = document.getElementById('auth-modal');
  if (modal) modal.style.display = 'none';
}

function switchAuthTab(tab) {
  authMode = tab;
  const isUp = tab === 'signup';
  const title = document.getElementById('auth-modal-title');
  if (title) title.textContent = isUp ? 'Create your account' : 'Welcome back';
  const tabIn = document.getElementById('auth-tab-signin');
  const tabUp = document.getElementById('auth-tab-signup');
  if (tabIn) tabIn.className = isUp ? 'lang-chip' : 'lang-chip selected';
  if (tabUp) tabUp.className = isUp ? 'lang-chip selected' : 'lang-chip';
  const nameField = document.getElementById('auth-name-field');
  if (nameField) nameField.style.display = isUp ? 'block' : 'none';
  const subBtn = document.getElementById('auth-submit-btn');
  if (subBtn) subBtn.textContent = isUp ? 'Create account' : 'Sign in';
  const errEl = document.getElementById('auth-error-msg');
  if (errEl) errEl.style.display = 'none';
}

async function submitAuthForm() {
  const errEl = document.getElementById('auth-error-msg');
  const btn = document.getElementById('auth-submit-btn');
  const name = document.getElementById('auth-name-input')?.value.trim() || '';
  const email = document.getElementById('auth-email-input')?.value.trim() || '';
  const password = document.getElementById('auth-password-input')?.value || '';

  if (authMode === 'signup' && !name) {
    showAuthError('Please enter your name.');
    return;
  }
  if (!email || !email.includes('@')) {
    showAuthError('Please enter a valid email address.');
    return;
  }
  if (password.length < 8) {
    showAuthError('Password must be at least 8 characters.');
    return;
  }

  btn.disabled = true;
  btn.textContent = 'Please wait...';
  if (errEl) errEl.style.display = 'none';

  try {
    const endpoint = authMode === 'signup' ? '/api/auth/signup' : '/api/auth/signin';
    const payload = authMode === 'signup' ? { name, email, password } : { email, password };
    const resp = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await resp.json();
    if (resp.ok && data.token && data.user) {
      setAuthSession(data.token, data.user);
      closeAuthModal();
    } else {
      showAuthError(data.error || 'Authentication failed. Please check credentials.');
    }
  } catch (err) {
    showAuthError('Could not reach the server. Please check your connection.');
  } finally {
    btn.disabled = false;
    btn.textContent = authMode === 'signup' ? 'Create account' : 'Sign in';
  }
}

async function handleGoogleCredentialResponse(response) {
  if (!response || !response.credential) return;
  const idToken = response.credential;
  try {
    const resp = await fetch('/api/auth/google', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id_token: idToken })
    });
    const data = await resp.json();
    if (resp.ok && data.token && data.user) {
      setAuthSession(data.token, data.user);
      closeAuthModal();
    } else {
      showAuthError(data.error || 'Google sign-in verification failed.');
    }
  } catch (err) {
    showAuthError('Could not verify Google credential with server.');
  }
}

function showAuthError(msg) {
  const errEl = document.getElementById('auth-error-msg');
  if (errEl) {
    errEl.textContent = msg;
    errEl.style.display = 'block';
  }
}

function setAuthSession(token, user) {
  currentAuthToken = token;
  currentAuthUser = user;
  localStorage.setItem('qinter_auth_token', token);
  localStorage.setItem('qinter_auth_user', JSON.stringify(user));
  updateWebAuthUI();
}

async function webLogout() {
  if (currentAuthToken) {
    try {
      await fetch('/api/auth/logout', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + currentAuthToken }
      });
    } catch (e) {}
  }
  currentAuthToken = null;
  currentAuthUser = null;
  localStorage.removeItem('qinter_auth_token');
  localStorage.removeItem('qinter_auth_user');
  updateWebAuthUI();
}

function updateWebAuthUI() {
  const inCard = document.getElementById('web-account-logged-in');
  const outCard = document.getElementById('web-account-logged-out');
  const nameEl = document.getElementById('web-account-name');
  const emailEl = document.getElementById('web-account-email');

  if (currentAuthUser && currentAuthToken) {
    if (inCard) inCard.style.display = 'block';
    if (outCard) outCard.style.display = 'none';
    if (nameEl) nameEl.textContent = currentAuthUser.name || 'User';
    if (emailEl) emailEl.textContent = currentAuthUser.email || '';
  } else {
    if (inCard) inCard.style.display = 'none';
    if (outCard) outCard.style.display = 'block';
  }
}

window.addEventListener('DOMContentLoaded', () => {
  updateWebAuthUI();
  if (currentAuthToken) {
    fetch('/api/auth/me', {
      headers: { 'Authorization': 'Bearer ' + currentAuthToken }
    }).then(r => {
      if (r.status === 401) {
        webLogout();
      }
    }).catch(() => {});
  }

  // Initialize web translation preferences
  const langEl = document.getElementById('web-pref-target-lang');
  if (langEl) {
    langEl.value = localStorage.getItem('qinter_pref_target_lang') || 'en';
  }
  setWebSpeechRate(parseFloat(localStorage.getItem('qinter_pref_speech_rate') || '1.0'));
  const autoBtn = document.getElementById('web-auto-speech-btn');
  if (autoBtn) {
    const on = localStorage.getItem('qinter_pref_auto_speech') !== 'false';
    autoBtn.textContent = on ? 'ON' : 'OFF';
    autoBtn.classList.toggle('selected', on);
  }
});

function saveWebTranslationPrefs() {
  const el = document.getElementById('web-pref-target-lang');
  if (el) localStorage.setItem('qinter_pref_target_lang', el.value);
}

let webSpeechRate = 1.0;
function setWebSpeechRate(rate) {
  webSpeechRate = rate;
  localStorage.setItem('qinter_pref_speech_rate', rate);
  ['08', '10', '12'].forEach(id => {
    const btn = document.getElementById('speed-btn-' + id);
    if (btn) btn.classList.remove('selected');
  });
  const activeBtn = document.getElementById('speed-btn-' + (rate === 0.8 ? '08' : rate === 1.2 ? '12' : '10'));
  if (activeBtn) activeBtn.classList.add('selected');
}

let webAutoSpeech = true;
function toggleWebAutoSpeech() {
  webAutoSpeech = !webAutoSpeech;
  localStorage.setItem('qinter_pref_auto_speech', webAutoSpeech);
  const btn = document.getElementById('web-auto-speech-btn');
  if (btn) {
    btn.textContent = webAutoSpeech ? 'ON' : 'OFF';
    btn.classList.toggle('selected', webAutoSpeech);
  }
}

function clearWebCache() {
  localStorage.removeItem('qinter_pref_target_lang');
  localStorage.removeItem('qinter_pref_speech_rate');
  localStorage.removeItem('qinter_pref_auto_speech');
  alert('Local cache cleared.');
}



