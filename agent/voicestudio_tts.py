"""
Qinter AI - VoiceStudio Hybrid TTS Engine
Integrates debpalash/VoiceStudio local engine (OmniVoice / Sherpa-ONNX / Kokoro on port 3900)
with built-in Neural TTS (edge-tts) across all 20 supported languages.
Supports 4 Voice Presets (Default: 'male_natural'):
  - male_natural   (Default Male Natural)
  - male_deep      (Male Deep Authoritative)
  - female_natural (Female Natural Clear)
  - female_warm    (Female Warm Expressive)
"""

import os
import io
import json
import asyncio
import urllib.request
import urllib.parse
from typing import Dict, Tuple, Optional

# Default voice preset is Male Natural
DEFAULT_VOICE_PRESET = "male_natural"

# Prosody adjustments per preset (rate_offset_pct, pitch_offset_hz)
PRESET_PROSODY: Dict[str, Tuple[int, str]] = {
    "male_natural": (0, "+0Hz"),
    "male_deep": (-4, "-8Hz"),
    "female_natural": (0, "+0Hz"),
    "female_warm": (-2, "-3Hz"),
}

# Complete 20-Language Neural Voice Map for all 4 presets
# Every language in Qinter AI (en, ar, fr, es, de, it, pt, ru, zh, ja, ko, hi, tr, nl, pl, sv, fa, ur, he, id)
VOICE_MAP: Dict[str, Dict[str, str]] = {
    "en": {
        "male_natural": "en-US-AndrewNeural",
        "male_deep": "en-US-ChristopherNeural",
        "female_natural": "en-US-AvaNeural",
        "female_warm": "en-US-EmmaNeural",
    },
    "ar": {
        "male_natural": "ar-SA-HamedNeural",
        "male_deep": "ar-EG-ShakirNeural",
        "female_natural": "ar-SA-ZariyahNeural",
        "female_warm": "ar-EG-SalmaNeural",
    },
    "fr": {
        "male_natural": "fr-FR-HenriNeural",
        "male_deep": "fr-CA-AntoineNeural",
        "female_natural": "fr-FR-DeniseNeural",
        "female_warm": "fr-FR-VivienneMultilingualNeural",
    },
    "es": {
        "male_natural": "es-ES-AlvaroNeural",
        "male_deep": "es-MX-JorgeNeural",
        "female_natural": "es-ES-ElviraNeural",
        "female_warm": "es-MX-DaliaNeural",
    },
    "de": {
        "male_natural": "de-DE-ConradNeural",
        "male_deep": "de-DE-KillianNeural",
        "female_natural": "de-DE-KatjaNeural",
        "female_warm": "de-DE-AmalaNeural",
    },
    "it": {
        "male_natural": "it-IT-DiegoNeural",
        "male_deep": "it-IT-GiuseppeNeural",
        "female_natural": "it-IT-ElsaNeural",
        "female_warm": "it-IT-IsabellaNeural",
    },
    "pt": {
        "male_natural": "pt-BR-AntonioNeural",
        "male_deep": "pt-PT-DuarteNeural",
        "female_natural": "pt-BR-FranciscaNeural",
        "female_warm": "pt-PT-RaquelNeural",
    },
    "ru": {
        "male_natural": "ru-RU-DmitryNeural",
        "male_deep": "ru-RU-DmitryNeural",
        "female_natural": "ru-RU-SvetlanaNeural",
        "female_warm": "ru-RU-SvetlanaNeural",
    },
    "zh": {
        "male_natural": "zh-CN-YunxiNeural",
        "male_deep": "zh-CN-YunjianNeural",
        "female_natural": "zh-CN-XiaoxiaoNeural",
        "female_warm": "zh-CN-XiaoyiNeural",
    },
    "ja": {
        "male_natural": "ja-JP-KeitaNeural",
        "male_deep": "ja-JP-KeitaNeural",
        "female_natural": "ja-JP-NanamiNeural",
        "female_warm": "ja-JP-NanamiNeural",
    },
    "ko": {
        "male_natural": "ko-KR-InJoonNeural",
        "male_deep": "ko-KR-HyunsuNeural",
        "female_natural": "ko-KR-SunHiNeural",
        "female_warm": "ko-KR-SunHiNeural",
    },
    "hi": {
        "male_natural": "hi-IN-MadhurNeural",
        "male_deep": "hi-IN-MadhurNeural",
        "female_natural": "hi-IN-SwaraNeural",
        "female_warm": "hi-IN-SwaraNeural",
    },
    "tr": {
        "male_natural": "tr-TR-AhmetNeural",
        "male_deep": "tr-TR-AhmetNeural",
        "female_natural": "tr-TR-EmelNeural",
        "female_warm": "tr-TR-EmelNeural",
    },
    "nl": {
        "male_natural": "nl-NL-MaartenNeural",
        "male_deep": "nl-NL-MaartenNeural",
        "female_natural": "nl-NL-ColetteNeural",
        "female_warm": "nl-NL-FennaNeural",
    },
    "pl": {
        "male_natural": "pl-PL-MarekNeural",
        "male_deep": "pl-PL-MarekNeural",
        "female_natural": "pl-PL-ZofiaNeural",
        "female_warm": "pl-PL-ZofiaNeural",
    },
    "sv": {
        "male_natural": "sv-SE-MattiasNeural",
        "male_deep": "sv-SE-MattiasNeural",
        "female_natural": "sv-SE-SofieNeural",
        "female_warm": "sv-SE-HilleviNeural",
    },
    "fa": {
        "male_natural": "fa-IR-FaridNeural",
        "male_deep": "fa-IR-FaridNeural",
        "female_natural": "fa-IR-DilaraNeural",
        "female_warm": "fa-IR-DilaraNeural",
    },
    "ur": {
        "male_natural": "ur-PK-AsadNeural",
        "male_deep": "ur-IN-SalmanNeural",
        "female_natural": "ur-PK-UzmaNeural",
        "female_warm": "ur-IN-GulNeural",
    },
    "he": {
        "male_natural": "he-IL-AvriNeural",
        "male_deep": "he-IL-AvriNeural",
        "female_natural": "he-IL-HilaNeural",
        "female_warm": "he-IL-HilaNeural",
    },
    "id": {
        "male_natural": "id-ID-ArdiNeural",
        "male_deep": "id-ID-ArdiNeural",
        "female_natural": "id-ID-GadisNeural",
        "female_warm": "id-ID-GadisNeural",
    },
}

_TTS_CACHE: Dict[str, bytes] = {}
_MAX_CACHE_ITEMS = 200


def normalize_lang(lang: str) -> str:
    if not lang:
        return "en"
    c = lang.strip().lower()
    if c.startswith("zh"):
        return "zh"
    if "-" in c:
        c = c.split("-")[0]
    if "_" in c:
        c = c.split("_")[0]
    return c if c in VOICE_MAP else "en"


def normalize_preset(preset: str) -> str:
    if not preset:
        return DEFAULT_VOICE_PRESET
    p = preset.strip().lower()
    if p in ("male", "man", "men", "male_natural"):
        return "male_natural"
    if p in ("male_deep", "deep"):
        return "male_deep"
    if p in ("female", "woman", "women", "female_natural"):
        return "female_natural"
    if p in ("female_warm", "warm"):
        return "female_warm"
    return DEFAULT_VOICE_PRESET


def _try_local_voicestudio(text: str, lang: str, preset: str, rate: float) -> Optional[bytes]:
    """
    Tier 1: Connect to local debpalash/VoiceStudio backend (default http://127.0.0.1:3900)
    if running on local workstation or configured via VOICESTUDIO_API_URL.
    """
    vs_url = os.getenv("VOICESTUDIO_API_URL", "").strip()
    if not vs_url:
        # Only probe localhost:3900 if explicitly enabled or VOICESTUDIO_ENABLE_LOCAL=1
        if os.getenv("VOICESTUDIO_ENABLE_LOCAL", "0") != "1":
            return None
        vs_url = "http://127.0.0.1:3900"

    gender = "female" if preset.startswith("female") else "male"
    payload = json.dumps({
        "text": text,
        "language": lang,
        "voice_preset": preset,
        "gender": gender,
        "speed": rate,
        "engine": os.getenv("OMNIVOICE_TTS_BACKEND", "omnivoice")
    }).encode("utf-8")

    for endpoint in ("/api/tts", "/api/generate_speech", "/v1/audio/speech"):
        try:
            req = urllib.request.Request(
                f"{vs_url.rstrip('/')}{endpoint}",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=2.5) as resp:
                if resp.status == 200:
                    data = resp.read()
                    if data and len(data) > 256:
                        return data
        except Exception:
            continue
    return None


async def _synthesize_edge_tts_async(text: str, voice_id: str, rate_str: str, pitch_str: str) -> bytes:
    import edge_tts
    communicate = edge_tts.Communicate(text=text, voice=voice_id, rate=rate_str, pitch=pitch_str)
    buf = io.BytesIO()
    async for chunk in communicate.stream():
        if chunk.get("type") == "audio" and chunk.get("data"):
            buf.write(chunk["data"])
    return buf.getvalue()


def synthesize_speech(
    text: str,
    lang: str = "en",
    voice_preset: str = DEFAULT_VOICE_PRESET,
    rate: float = 1.0
) -> Optional[bytes]:
    """
    Synthesizes MP3 audio using the VoiceStudio Hybrid Architecture:
      1. Local debpalash/VoiceStudio instance (OmniVoice / Sherpa-ONNX / Kokoro)
      2. Neural TTS (edge-tts) with 20-language Male/Female voice presets
    """
    clean_text = (text or "").strip()[:400]
    if not clean_text:
        return None

    norm_lang = normalize_lang(lang)
    norm_preset = normalize_preset(voice_preset)
    try:
        clamped_rate = max(0.6, min(1.5, float(rate)))
    except Exception:
        clamped_rate = 1.0

    cache_key = f"{norm_lang}_{norm_preset}_{clamped_rate:.2f}_{clean_text.lower()}"
    if cache_key in _TTS_CACHE:
        return _TTS_CACHE[cache_key]

    # Tier 1: Local debpalash/VoiceStudio Engine
    vs_bytes = _try_local_voicestudio(clean_text, norm_lang, norm_preset, clamped_rate)
    if vs_bytes:
        if len(_TTS_CACHE) >= _MAX_CACHE_ITEMS:
            _TTS_CACHE.pop(next(iter(_TTS_CACHE)), None)
        _TTS_CACHE[cache_key] = vs_bytes
        return vs_bytes

    # Tier 2: Neural TTS (edge-tts) with Male/Female Preset & Prosody
    lang_voices = VOICE_MAP.get(norm_lang, VOICE_MAP["en"])
    voice_id = lang_voices.get(norm_preset, lang_voices[DEFAULT_VOICE_PRESET])

    base_rate_offset, pitch_str = PRESET_PROSODY.get(norm_preset, (0, "+0Hz"))
    user_rate_offset = int(round((clamped_rate - 1.0) * 100))
    total_rate_pct = max(-50, min(60, base_rate_offset + user_rate_offset))
    rate_str = f"{total_rate_pct:+d}%"

    try:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    audio_bytes = pool.submit(
                        lambda: asyncio.run(_synthesize_edge_tts_async(clean_text, voice_id, rate_str, pitch_str))
                    ).result(timeout=8)
            else:
                audio_bytes = loop.run_until_complete(
                    _synthesize_edge_tts_async(clean_text, voice_id, rate_str, pitch_str)
                )
        except RuntimeError:
            audio_bytes = asyncio.run(_synthesize_edge_tts_async(clean_text, voice_id, rate_str, pitch_str))

        if audio_bytes and len(audio_bytes) > 200:
            if len(_TTS_CACHE) >= _MAX_CACHE_ITEMS:
                _TTS_CACHE.pop(next(iter(_TTS_CACHE)), None)
            _TTS_CACHE[cache_key] = audio_bytes
            return audio_bytes
    except Exception as e:
        print(f"[VoiceStudio TTS] Neural synthesis error ({voice_id}): {e}", flush=True)

    return None
