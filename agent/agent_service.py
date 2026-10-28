"""
Qinter AI - LangGraph Autonomous Agent Service
Intelligent Load-Balancing & Circuit Breaker via Redis:
  • Primary Tier: NVIDIA NIM Enterprise Cloud API (Ultra-Fast Nemotron 3.5 Bolt)
  • Local Failover Tier: Ollama LLM (Qwen 2.5 3B / LLaMA 3.2 3B) on VPS / Localhost
  • State & Cache Engine: Redis for health tracking, concurrency counters, and semantic cache
"""

import sys
import os
import json
import re
import time
import hashlib
import argparse
import urllib.request
import urllib.parse
import urllib.error
from html import unescape
from pathlib import Path

# Ensure UTF-8 console output on Windows
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from typing import TypedDict, List, Dict, Any, Optional, Tuple

# 1. Load .env Configuration
ENV_FILE = Path(__file__).parent / ".env"
if ENV_FILE.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=ENV_FILE, override=True)
    except Exception:
        # Built-in lightweight fallback parser if dotenv package is not yet loaded
        try:
            with open(ENV_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        os.environ[k] = v
        except Exception:
            pass

sys.path.insert(0, str(Path(__file__).parent))
import auth_service  # account system (email/password + Google sign-in)

try:
    from langgraph.graph import StateGraph, START, END
except ImportError as e:
    print(f"Error importing LangGraph: {e}", file=sys.stderr)
    print("Please run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)


# ==============================================================================
# REDIS LOAD BALANCER & CIRCUIT BREAKER ENGINE
# ==============================================================================
class RedisLoadBalancer:
    """
    Manages active request counters, health status, circuit breaking,
    and response caching between NVIDIA NIM and Ollama.
    """
    def __init__(self, redis_url: Optional[str] = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
        self.client = None
        self.has_redis = False

        # In-memory backup state if Redis server is temporarily unreachable
        self._mem_circuit_state = "CLOSED"
        self._mem_cooldown_until = 0.0
        self._mem_active_nim = 0
        self._mem_cache: Dict[str, Tuple[str, float]] = {}
        self._mem_stats = {
            "nim_success": 0,
            "nim_fail": 0,
            "ollama_success": 0,
            "cache_hits": 0
        }

        self._connect()

    def _connect(self):
        try:
            import redis
            self.client = redis.Redis.from_url(
                self.redis_url,
                socket_timeout=1.5,
                socket_connect_timeout=1.5,
                decode_responses=True
            )
            self.client.ping()
            self.has_redis = True
            print(f"✓ [Redis LB] Connected to Redis at {self.redis_url}")
        except Exception as e:
            self.has_redis = False
            self.client = None
            print(f"ℹ [Redis LB] Redis server notice ({e}). Operating in resilient in-memory mode.")

    def get_circuit_state(self) -> Tuple[str, float]:
        """Returns (circuit_state, cooldown_remaining_seconds)"""
        now = time.time()
        if self.has_redis:
            try:
                state = self.client.get("qinter:circuit:nim_state") or "CLOSED"
                cooldown = float(self.client.get("qinter:circuit:nim_cooldown") or 0.0)
                rem = max(0.0, cooldown - now)
                return state, rem
            except Exception:
                pass
        rem = max(0.0, self._mem_cooldown_until - now)
        return self._mem_circuit_state, rem

    def set_circuit_state(self, state: str, cooldown_duration: float = 30.0):
        now = time.time()
        cooldown_until = now + cooldown_duration if cooldown_duration > 0 else 0.0
        if self.has_redis:
            try:
                self.client.set("qinter:circuit:nim_state", state)
                self.client.set("qinter:circuit:nim_cooldown", str(cooldown_until))
            except Exception:
                pass
        self._mem_circuit_state = state
        self._mem_cooldown_until = cooldown_until

    def choose_target_backend(self, mode_requested: str, nim_key: str) -> str:
        """
        Determines whether to dispatch to NVIDIA NIM or Ollama.
        If mode is AUTO_BALANCED (or default):
          1. Verifies NIM key availability.
          2. Verifies Circuit Breaker (if tripped, route to Ollama).
          3. Checks concurrent load on NIM (if capacity threshold reached, offload to Ollama).
        """
        if mode_requested == "VPS_LOCAL_LLM":
            return "VPS_LOCAL_LLM"
        if mode_requested == "NVIDIA_NIM":
            return "NVIDIA_NIM"

        # Mode is AUTO_BALANCED:
        effective_key = nim_key.strip() or os.getenv("NVIDIA_NIM_API_KEY", "").strip()
        if not effective_key:
            return "VPS_LOCAL_LLM"

        circuit_state, rem = self.get_circuit_state()
        if circuit_state == "OPEN":
            if rem > 0:
                print(f"⚡ [Load Balancer] NIM circuit is OPEN (cooldown {rem:.1f}s). Routing to Ollama.")
                return "VPS_LOCAL_LLM"
            else:
                self.set_circuit_state("HALF_OPEN", 0.0)

        # Check concurrency threshold
        max_concurrent = int(os.getenv("NIM_MAX_CONCURRENT", "5"))
        active = self.get_active_nim_requests()
        if active >= max_concurrent:
            print(f"⚡ [Load Balancer] NIM saturated ({active}/{max_concurrent} active). Offloading to Ollama.")
            return "VPS_LOCAL_LLM"

        return "NVIDIA_NIM"

    def get_active_nim_requests(self) -> int:
        if self.has_redis:
            try:
                return int(self.client.get("qinter:lb:nim_active") or 0)
            except Exception:
                pass
        return self._mem_active_nim

    def record_nim_start(self):
        if self.has_redis:
            try:
                self.client.incr("qinter:lb:nim_active")
            except Exception:
                pass
        self._mem_active_nim += 1

    def record_nim_end(self, success: bool, error_msg: str = ""):
        if self.has_redis:
            try:
                val = self.client.decr("qinter:lb:nim_active")
                if val < 0:
                    self.client.set("qinter:lb:nim_active", "0")
                if success:
                    self.client.incr("qinter:stats:nim_success")
                    self.client.set("qinter:circuit:nim_state", "CLOSED")
                    self.client.set("qinter:circuit:nim_errors", "0")
                else:
                    self.client.incr("qinter:stats:nim_fail")
                    errs = self.client.incr("qinter:circuit:nim_errors")
                    if errs >= 2:
                        cooldown = float(os.getenv("NIM_CIRCUIT_COOLDOWN", "30"))
                        self.set_circuit_state("OPEN", cooldown)
            except Exception:
                pass

        self._mem_active_nim = max(0, self._mem_active_nim - 1)
        if success:
            self._mem_stats["nim_success"] += 1
            self._mem_circuit_state = "CLOSED"
        else:
            self._mem_stats["nim_fail"] += 1
            self.set_circuit_state("OPEN", 30.0)

    def record_ollama_success(self):
        if self.has_redis:
            try:
                self.client.incr("qinter:stats:ollama_success")
            except Exception:
                pass
        self._mem_stats["ollama_success"] += 1

    def get_cache(self, key: str) -> Optional[str]:
        if self.has_redis:
            try:
                cached = self.client.get(f"qinter:cache:{key}")
                if cached:
                    self.client.incr("qinter:stats:cache_hits")
                    return cached
            except Exception:
                pass
        now = time.time()
        if key in self._mem_cache:
            val, exp = self._mem_cache[key]
            if now < exp:
                self._mem_stats["cache_hits"] += 1
                return val
            else:
                del self._mem_cache[key]
        return None

    def set_cache(self, key: str, value: str, ttl: int = 3600):
        if not value or len(value) < 10:
            return
        if self.has_redis:
            try:
                self.client.set(f"qinter:cache:{key}", value, ex=ttl)
            except Exception:
                pass
        self._mem_cache[key] = (value, time.time() + ttl)

    def get_telemetry(self) -> Dict[str, Any]:
        circuit, rem = self.get_circuit_state()
        active = self.get_active_nim_requests()
        if self.has_redis:
            try:
                return {
                    "redis_connected": True,
                    "circuit_state": circuit,
                    "cooldown_remaining_sec": round(rem, 1),
                    "active_nim_requests": active,
                    "stats": {
                        "nim_success": int(self.client.get("qinter:stats:nim_success") or 0),
                        "nim_fail": int(self.client.get("qinter:stats:nim_fail") or 0),
                        "ollama_success": int(self.client.get("qinter:stats:ollama_success") or 0),
                        "cache_hits": int(self.client.get("qinter:stats:cache_hits") or 0)
                    }
                }
            except Exception:
                pass
        return {
            "redis_connected": False,
            "circuit_state": circuit,
            "cooldown_remaining_sec": round(rem, 1),
            "active_nim_requests": active,
            "stats": self._mem_stats
        }


# Global Load Balancer Instance
LOAD_BALANCER = RedisLoadBalancer()


# ==============================================================================
# LANGGRAPH STATE & GRAPH DEFINITION
# ==============================================================================
class AgentState(TypedDict):
    prompt: str
    transcript: str
    conversation_history: List[Dict[str, str]]
    mode: str                    # 'AUTO_BALANCED', 'VPS_LOCAL_LLM', or 'NVIDIA_NIM'
    vps_host: str                # e.g. '127.0.0.1:11434'
    vps_model: str               # e.g. 'qwen2.5:3b'
    nim_api_key: str             # 'nvapi-...'
    nim_model: str               # e.g. 'nvidia/nemotron-3.5-bolt-e4b'
    detected_language: str
    retrieved_context: str
    search_web: bool
    web_sources: List[Dict[str, str]]
    web_context: str
    final_response: str
    citations: List[Dict[str, str]]
    backend_used: str


def clean_endpoint(host: str) -> str:
    """Ensure host has http:// scheme."""
    h = host.strip()
    if not h.startswith("http://") and not h.startswith("https://"):
        h = f"http://{h}"
    return h.rstrip("/")


def resolve_ollama_endpoint(host_input: str) -> str:
    """
    Ensure we connect to Ollama (typically port 11434) and NEVER accidentally
    loop back to the LangGraph agent server (port 8000).
    """
    env_ollama = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434").strip()
    if not host_input:
        return clean_endpoint(env_ollama)

    clean = host_input.strip()
    if ":8000" in clean:
        ip = clean.replace("http://", "").replace("https://", "").split(":")[0]
        if ip in ["127.0.0.1", "localhost", "10.0.2.2", "0.0.0.0"]:
            return clean_endpoint(env_ollama)
        return clean_endpoint(f"http://{ip}:11434")

    if ":11434" in clean:
        return clean_endpoint(clean)

    return clean_endpoint(env_ollama)


def get_installed_ollama_models(ollama_url: str) -> List[str]:
    """Dynamically fetch all currently available model tags from local Ollama."""
    try:
        base = clean_endpoint(ollama_url).replace("/v1", "").rstrip("/")
        req = urllib.request.Request(f"{base}/api/tags", headers={"User-Agent": "QinterAgent"})
        with urllib.request.urlopen(req, timeout=2.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return [m["name"] for m in data.get("models", [])]
    except Exception:
        return []


def call_nvidia_nim_http(
    messages: List[Dict[str, str]],
    model: str,
    api_key: str,
    max_tokens: int = 500,
    timeout: float = 12.0
) -> str:
    """
    Direct HTTP REST call to NVIDIA NIM cloud API. Zero OpenAI dependencies.
    Extracts response content or reasoning_content seamlessly.
    """
    base_url = os.getenv("NVIDIA_NIM_BASE_URL", "https://integrate.api.nvidia.com/v1").rstrip("/")
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.6,
        "max_tokens": max_tokens
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "QinterAgent/2.0"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        res = json.loads(resp.read().decode("utf-8"))
        choices = res.get("choices", [])
        if not choices:
            raise ValueError("Empty choices array from NVIDIA NIM")
        msg = choices[0].get("message", {})
        content = (msg.get("content") or msg.get("reasoning_content") or "").strip()
        if not content:
            raise ValueError("No text content returned by NVIDIA NIM")
        return content


def call_ollama_http(
    messages: List[Dict[str, str]],
    model: str,
    host: str,
    max_tokens: int = 500,
    timeout: float = 45.0
) -> str:
    """
    Direct HTTP REST call to Ollama (via native /api/chat or /v1/chat/completions).
    Zero OpenAI dependencies.
    """
    base = clean_endpoint(host).replace("/v1", "").rstrip("/")
    # 1. Native Ollama /api/chat
    native_url = f"{base}/api/chat"
    native_payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.6,
            "num_predict": max_tokens
        }
    }
    try:
        req = urllib.request.Request(
            native_url,
            data=json.dumps(native_payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "QinterAgent/2.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            res = json.loads(resp.read().decode("utf-8"))
            content = res.get("message", {}).get("content", "").strip()
            if content:
                return content
    except Exception:
        pass

    # 2. Fallback to /v1/chat/completions on Ollama
    v1_url = f"{base}/v1/chat/completions"
    v1_payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.6,
        "max_tokens": max_tokens
    }
    req2 = urllib.request.Request(
        v1_url,
        data=json.dumps(v1_payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "QinterAgent/2.0"}
    )
    with urllib.request.urlopen(req2, timeout=timeout) as resp2:
        res2 = json.loads(resp2.read().decode("utf-8"))
        choices = res2.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            content = (msg.get("content") or "").strip()
            if content:
                return content

    raise ValueError(f"Could not get response from Ollama at {host} for model {model}")


def call_cloud_llm_http(
    messages: List[Dict[str, str]],
    model: str = "openai",
    max_tokens: int = 500,
    timeout: float = 18.0
) -> str:
    """
    Ultra-reliable, zero-key cloud inference fallback (Pollinations AI).
    Guarantees AI assistant logic works on cloud containers (e.g. Render/Railway)
    even when local Ollama is not running and no custom NIM API key is provided.
    """
    url = "https://text.pollinations.ai/"
    payload = {
        "messages": messages,
        "model": model,
        "temperature": 0.6,
        "max_tokens": max_tokens
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        content = resp.read().decode("utf-8").strip()
        if content:
            return content
    raise ValueError("Empty response from cloud fallback")


def detect_language_and_intent(state: AgentState) -> Dict[str, Any]:
    prompt = state["prompt"].strip()

    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F]')
    chinese_pattern = re.compile(r'[\u4E00-\u9FFF]')
    cyrillic_pattern = re.compile(r'[\u0400-\u04FF]')
    japanese_pattern = re.compile(r'[\u3040-\u309F\u30A0-\u30FF]')

    if arabic_pattern.search(prompt):
        lang = "Arabic"
    elif chinese_pattern.search(prompt):
        lang = "Chinese"
    elif cyrillic_pattern.search(prompt):
        lang = "Russian"
    elif japanese_pattern.search(prompt):
        lang = "Japanese"
    else:
        lower = f" {prompt.lower()} "
        if any(w in lower for w in [" le ", " la ", " les ", " est ", " du ", " dans ", " pour ", " que ", " qui ", " quoi ", " comment ", " pourquoi ", " explique ", " résume "]):
            lang = "French"
        elif any(w in lower for w in [" el ", " la ", " los ", " las ", " en ", " por ", " qué ", " cómo ", " resumen ", " explica ", " sobre "]):
            lang = "Spanish"
        elif any(w in lower for w in [" der ", " die ", " das ", " und ", " ist ", " für ", " warum ", " wie ", " erkläre ", " zusammenfassung "]):
            lang = "German"
        else:
            lang = "User's input language (mirror exact language of prompt)"

    return {"detected_language": lang}


def retrieve_context(state: AgentState) -> Dict[str, Any]:
    transcript = state.get("transcript", "")
    prompt = state.get("prompt", "")

    if not transcript:
        return {"retrieved_context": "", "citations": []}

    lines = transcript.split("\n")
    keywords = [w.lower() for w in re.findall(r'\b\w{3,}\b', prompt)]

    relevant_lines = []
    citations = []

    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
        line_lower = line_clean.lower()
        ts_match = re.search(r'\[(\d{1,2}:\d{2}(?::\d{2})?)\]', line_clean)
        ts = ts_match.group(1) if ts_match else ""

        if any(kw in line_lower for kw in keywords):
            relevant_lines.append(line_clean)
            if ts:
                citations.append({"timestamp": ts, "snippet": line_clean[:120]})

    if not relevant_lines:
        context_str = transcript[:6000]
    else:
        context_str = "\n".join(relevant_lines[:30])

    return {"retrieved_context": context_str, "citations": citations[:8]}


def search_web_multi(query: str, max_results: int = 4) -> List[Dict[str, str]]:
    """
    Multi-engine real-time web search tool (DuckDuckGo Lite + Wikipedia API).
    Zero external pip dependencies required.
    """
    results: List[Dict[str, str]] = []
    seen_urls = set()
    clean_q = query.strip()
    if not clean_q:
        return results

    # 1. DuckDuckGo Lite Search
    try:
        data = urllib.parse.urlencode({"q": clean_q}).encode("utf-8")
        req = urllib.request.Request(
            "https://lite.duckduckgo.com/lite/",
            data=data,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
            blocks = re.findall(
                r"<a[^>]*href=['\"]([^'\"]+)['\"][^>]*class=['\"]result-link['\"][^>]*>(.*?)</a>.*?<td[^>]*class=['\"]result-snippet['\"][^>]*>(.*?)</td>",
                html,
                re.DOTALL
            )
            for raw_url, raw_title, raw_snippet in blocks[:max_results]:
                actual_url = raw_url.strip()
                if "uddg=" in actual_url:
                    m = re.search(r'uddg=([^&]+)', actual_url)
                    if m:
                        actual_url = urllib.parse.unquote(m.group(1))

                if actual_url in seen_urls:
                    continue
                seen_urls.add(actual_url)

                clean_title = unescape(re.sub(r"<[^>]+>", "", raw_title)).strip()
                clean_snippet = unescape(re.sub(r"<[^>]+>", "", raw_snippet)).strip()
                clean_snippet = re.sub(r"\s+", " ", clean_snippet)

                if clean_title and clean_snippet:
                    results.append({
                        "title": clean_title,
                        "url": actual_url,
                        "snippet": clean_snippet
                    })
    except Exception as e:
        print(f"ℹ [Web Search] DuckDuckGo notice: {e}")

    # 2. Wikipedia Search Fallback or Augment
    if len(results) < 2:
        try:
            wiki_url = "https://en.wikipedia.org/w/api.php?action=query&list=search&srsearch=" + urllib.parse.quote_plus(clean_q) + "&utf8=&format=json"
            w_req = urllib.request.Request(wiki_url, headers={"User-Agent": "QinterAgent/2.0 (Web Search Tool)"})
            with urllib.request.urlopen(w_req, timeout=4.0) as w_resp:
                w_data = json.loads(w_resp.read().decode("utf-8"))
                for item in w_data.get("query", {}).get("search", [])[:3]:
                    title = item.get("title", "")
                    page_url = f"https://en.wikipedia.org/wiki/{urllib.parse.quote(title.replace(' ', '_'))}"
                    if page_url in seen_urls:
                        continue
                    seen_urls.add(page_url)
                    clean_snip = unescape(re.sub(r"<[^>]+>", "", item.get("snippet", ""))).strip()
                    clean_snip = re.sub(r"\s+", " ", clean_snip)
                    results.append({
                        "title": f"{title} - Wikipedia",
                        "url": page_url,
                        "snippet": clean_snip
                    })
        except Exception as we:
            print(f"ℹ [Web Search] Wikipedia notice: {we}")

    return results


def should_trigger_web_search(prompt: str, search_web_flag: bool) -> bool:
    if search_web_flag:
        return True
    lower = f" {prompt.lower()} "
    search_keywords = [
        " search ", " recherche ", " cherche ", " web ", " google ", " internet ",
        " latest ", " actualite ", " actualité ", " news ", " dernier ", " dernière ",
        " who is ", " qui est ", " who won ", " qui a gagne ", " qui a gagné ",
        " what is the price ", " quel est le prix ", " meteo ", " météo ", " weather ",
        " score ", " resultat ", " résultat ", " today ", " aujourd'hui ",
        " release date ", " date de sortie ", " 2024 ", " 2025 ", " 2026 "
    ]
    return any(kw in lower for kw in search_keywords)


def web_search_node(state: AgentState) -> Dict[str, Any]:
    prompt = state.get("prompt", "").strip()
    search_web_flag = state.get("search_web", False)

    if not should_trigger_web_search(prompt, search_web_flag):
        return {"web_sources": [], "web_context": ""}

    query = prompt
    for prefix in ["search for", "search", "recherche sur", "recherche", "cherche sur", "cherche", "look up", "find information about", "find"]:
        if query.lower().startswith(prefix):
            query = query[len(prefix):].strip(" :,-")
            break

    print(f"🌐 [LangGraph Tool] Executing Live Web Search: \"{query}\"")
    sources = search_web_multi(query, max_results=4)
    if not sources:
        return {"web_sources": [], "web_context": ""}

    lines = [f"--- Live Web Search Results for \"{query}\" ---"]
    for i, s in enumerate(sources, 1):
        lines.append(f"[{i}] [{s['title']}]({s['url']})")
        lines.append(f"    Snippet: {s['snippet']}")

    web_context = "\n".join(lines)
    return {
        "web_sources": sources,
        "web_context": web_context
    }


def synthesize_response(state: AgentState) -> Dict[str, Any]:
    prompt = state["prompt"].strip()
    transcript = state.get("transcript", "")
    context = state.get("retrieved_context", "") or transcript
    mode_requested = state.get("mode", os.getenv("LOAD_BALANCING_MODE", "AUTO_BALANCED"))

    # Credentials resolution: request param -> .env -> defaults
    nim_key = state.get("nim_api_key", "").strip() or os.getenv("NVIDIA_NIM_API_KEY", "").strip()
    
    # Auto-upgrade outdated, slow, or timed-out models to the ultra-fast active model
    raw_nim_model = state.get("nim_model", "").strip() or os.getenv("NVIDIA_NIM_MODEL", "nvidia/nemotron-3.5-bolt-e4b")
    if any(dep in raw_nim_model for dep in ["llama-3.1-8b", "llama3-8b", "llama-3-8b", "llama-3.2-11b", "mixtral", "gemma", "120b"]):
        nim_model = "nvidia/nemotron-3.5-bolt-e4b"
    else:
        nim_model = raw_nim_model

    vps_host = resolve_ollama_endpoint(state.get("vps_host", ""))
    vps_model = state.get("vps_model", "").strip() or os.getenv("OLLAMA_MODEL", "llama3.1:8b")

    # 1. Check Redis Cache
    search_flag = "1" if state.get("search_web") else "0"
    cache_hash = hashlib.sha256(f"{prompt}::{transcript[:1000]}::{search_flag}".encode("utf-8")).hexdigest()
    cached_val = LOAD_BALANCER.get_cache(cache_hash)
    if cached_val:
        return {
            "final_response": cached_val,
            "backend_used": "Redis Cache"
        }

    # 2. Decide Target via Redis Load Balancer
    target_backend = LOAD_BALANCER.choose_target_backend(mode_requested, nim_key)

    # 3. Build comprehensive ChatGPT-level system instruction
    web_ctx = state.get("web_context", "").strip()
    doc_ctx = context.strip()

    context_blocks = []
    if doc_ctx:
        context_blocks.append(f"### Attached Document / Transcript Context:\n\"\"\"\n{doc_ctx}\n\"\"\"")
    if web_ctx:
        context_blocks.append(f"### Real-Time Live Web Search Findings:\n{web_ctx}")

    context_str = "\n\n".join(context_blocks)

    system_instruction = f"""You are Qinter AI, an advanced, highly versatile, and friendly multilingual AI assistant (providing ChatGPT-level conversational excellence, intellect, and nuance).

CORE CAPABILITIES & CONVERSATIONAL FREEDOM:
1. COMPREHENSIVE EXPERTISE: You excel at answering general questions, explanations, coding, writing, brainstorming, mathematics, and everyday chat.
2. CONTEXTUAL FLEXIBILITY:
   - If document or transcript context is provided below, treat it as helpful background. Seamlessly reference it when the user asks questions about it, requests summaries, or refers to specific points.
   - If the user asks general questions, coding problems, explanations, or creative tasks UNRELATED to the document, answer with complete freedom using your full general intelligence. NEVER say you can only answer from the document.
3. LIVE WEB KNOWLEDGE:
   - When Live Web Search Results are provided below, synthesize them naturally to provide up-to-date, accurate answers. Cite sources with Markdown links [Source Title](URL).
4. MULTILINGUAL FLUENCY:
   - Always respond in the EXACT language of the user's prompt (French, English, Arabic, Spanish, German, Chinese, etc.) with native flow and sophistication.
5. BEAUTIFUL PRESENTATION:
   - Use elegant Markdown formatting (clear headings, bullet points, bold key terms, and code blocks with syntax highlighting) for maximum readability.

{context_str}
"""
    chat_messages: List[Dict[str, str]] = [
        {"role": "system", "content": system_instruction}
    ]

    for msg in state.get("conversation_history", [])[-6:]:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if content:
            chat_messages.append({"role": role, "content": content})

    chat_messages.append({"role": "user", "content": prompt})

    # Execution Function for Ollama with dynamic model discovery and fallback
    def invoke_ollama() -> Tuple[str, str]:
        installed = get_installed_ollama_models(vps_host)
        candidates = []
        if vps_model in installed:
            candidates.append(vps_model)
        for m in installed:
            m_lower = m.lower()
            if "embed" in m_lower or "27b" in m_lower or "26b" in m_lower or "70b" in m_lower:
                continue
            candidates.append(m)
        candidates.extend(["llama3.1:8b", "qwen2.5-coder:1.5b-base", "llama3.2:3b", "qwen2.5:3b"])
        candidates = list(dict.fromkeys(candidates))

        last_err = None
        for cand in candidates:
            try:
                content = call_ollama_http(
                    messages=chat_messages,
                    model=cand,
                    host=vps_host,
                    max_tokens=500,
                    timeout=50.0
                )
                LOAD_BALANCER.record_ollama_success()
                return content, f"Ollama ({cand})"
            except Exception as e:
                last_err = e
                err_str = str(e).lower()
                if "not found" in err_str or "404" in err_str or "model" in err_str:
                    continue
                else:
                    raise e
        raise last_err or Exception("No compatible model found on Ollama")

    # Execution Function for NVIDIA NIM (Ultra-Fast 0.3s)
    def invoke_nim() -> Tuple[str, str]:
        LOAD_BALANCER.record_nim_start()
        try:
            content = call_nvidia_nim_http(
                messages=chat_messages,
                model=nim_model,
                api_key=nim_key,
                max_tokens=500,
                timeout=12.0
            )
            LOAD_BALANCER.record_nim_end(success=True)
            return content, f"NVIDIA NIM ({nim_model})"
        except Exception as e:
            LOAD_BALANCER.record_nim_end(success=False, error_msg=str(e))
            raise e

    # Execute with Automatic Load-Balancing & Failover
    try:
        content = ""
        backend = ""

        if target_backend == "NVIDIA_NIM" and nim_key:
            try:
                content, backend = invoke_nim()
            except Exception as nim_err:
                print(f"⚠️ [Failover] NVIDIA NIM error ({nim_err}). Trying local Ollama...")
                try:
                    content, backend = invoke_ollama()
                    backend = f"{backend} (Failover from NIM)"
                except Exception as ollama_err:
                    print(f"⚠️ [Failover] Ollama also unavailable ({ollama_err}). Using Cloud Intelligence Fallback...")
                    content = call_cloud_llm_http(chat_messages)
                    backend = "Cloud Intelligence (Zero-Key Failover)"
        else:
            # Local Ollama requested or primary
            try:
                content, backend = invoke_ollama()
            except Exception as ollama_err:
                print(f"⚠️ [Failover] Ollama unavailable ({ollama_err}). Checking NIM or Cloud Intelligence...")
                if nim_key:
                    try:
                        content, backend = invoke_nim()
                        backend = f"{backend} (Failover from Ollama)"
                    except Exception as nim_err2:
                        print(f"⚠️ [Failover] NIM failed ({nim_err2}). Using Cloud Intelligence Fallback...")
                        content = call_cloud_llm_http(chat_messages)
                        backend = "Cloud Intelligence (Zero-Key Failover)"
                else:
                    content = call_cloud_llm_http(chat_messages)
                    backend = "Cloud Intelligence (Auto Failover)"

        # Cache valid response in Redis
        LOAD_BALANCER.set_cache(cache_hash, content, ttl=int(os.getenv("REDIS_CACHE_TTL", "3600")))
        return {"final_response": content, "backend_used": backend}

    except Exception as final_err:
        err_msg = str(final_err)
        return {
            "final_response": f"⚠️ Agent Notice: Unable to generate response: {err_msg}",
            "backend_used": "Error"
        }


# Build StateGraph
def build_agent_graph():
    builder = StateGraph(AgentState)
    builder.add_node("detect_intent", detect_language_and_intent)
    builder.add_node("retrieve", retrieve_context)
    builder.add_node("web_search", web_search_node)
    builder.add_node("synthesize", synthesize_response)

    builder.add_edge(START, "detect_intent")
    builder.add_edge("detect_intent", "retrieve")
    builder.add_edge("retrieve", "web_search")
    builder.add_edge("web_search", "synthesize")
    builder.add_edge("synthesize", END)

    return builder.compile()


AGENT_GRAPH = build_agent_graph()


def run_agent_workflow(
    prompt: str,
    transcript: str = "",
    history: Optional[List[Dict[str, str]]] = None,
    search_web: bool = False,
    mode: str = "AUTO_BALANCED",
    vps_host: str = "127.0.0.1:11434",
    vps_model: str = "qwen2.5:3b",
    nim_api_key: str = "",
    nim_model: str = "nvidia/nemotron-3.5-bolt-e4b"
) -> Dict[str, Any]:
    initial_state: AgentState = {
        "prompt": prompt,
        "transcript": transcript,
        "conversation_history": history or [],
        "mode": mode,
        "vps_host": vps_host,
        "vps_model": vps_model,
        "nim_api_key": nim_api_key,
        "nim_model": nim_model,
        "detected_language": "",
        "retrieved_context": "",
        "search_web": search_web,
        "web_sources": [],
        "web_context": "",
        "final_response": "",
        "citations": [],
        "backend_used": ""
    }

    final_state = AGENT_GRAPH.invoke(initial_state)
    web_sources = final_state.get("web_sources", [])
    return {
        "reply": final_state.get("final_response", ""),
        "detectedLanguage": final_state.get("detected_language", "en"),
        "citations": final_state.get("citations", []),
        "backendUsed": final_state.get("backend_used", "Auto"),
        "webSources": web_sources,
        "webSearched": len(web_sources) > 0,
        "mode": mode
    }


# ==============================================================================
# MULTI-THREADED HTTP SERVER
# ==============================================================================
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def transcribe_audio_bytes(raw_audio: bytes, lang: str = "fr-FR") -> str:
    if not raw_audio or len(raw_audio) < 400:
        return ""
    in_path = None
    out_wav = None
    try:
        import subprocess
        import tempfile
        import speech_recognition as sr

        ext = ".webm"
        if raw_audio.startswith(b"OggS"):
            ext = ".ogg"
        elif raw_audio.startswith(b"RIFF"):
            ext = ".wav"
        elif raw_audio.startswith(b"\x1a\x45\xdf\xa3"):
            ext = ".webm"

        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as in_f:
            in_f.write(raw_audio)
            in_path = in_f.name

        out_wav = in_path + ".wav"
        proc = subprocess.run(
            ["ffmpeg", "-y", "-i", in_path, "-vn", "-ar", "16000", "-ac", "1", out_wav],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=8
        )
        if proc.returncode != 0 or not os.path.exists(out_wav):
            err_msg = proc.stderr.decode("utf-8", errors="replace")[:200]
            print(f"[ASR ffmpeg fail] ret={proc.returncode}, size={len(raw_audio)}, ext={ext}, err: {err_msg}", flush=True)
            return ""

        wav_size = os.path.getsize(out_wav) if os.path.exists(out_wav) else 0
        r = sr.Recognizer()
        with sr.AudioFile(out_wav) as source:
            audio = r.record(source)
            text = r.recognize_google(audio, language=lang)
            print(f"[ASR Success] in_bytes={len(raw_audio)}, wav_bytes={wav_size}, text='{text}'", flush=True)
            return text.strip()
    except sr.UnknownValueError:
        print(f"[ASR UnknownValue] Audio had no decipherable speech (size={len(raw_audio)}).", flush=True)
        return ""
    except Exception as e:
        print(f"[ASR Exception] {type(e).__name__}: {e}", flush=True)
        return ""
    finally:
        for p in [in_path, out_wav]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except Exception: pass


class AgentRequestHandler(BaseHTTPRequestHandler):
    def _set_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def do_OPTIONS(self):
        self.send_response(204)
        self._set_cors_headers()
        self.end_headers()

    def _send_json(self, status: int, payload: dict):
        try:
            self.send_response(status)
            self._set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            pass

    def do_GET(self):
        if self.path == "/api/auth/me":
            status, payload = auth_service.me(self.headers)
            self._send_json(status, payload)
            return
        if self.path in ["/health", "/", "/api/agent/stats"]:
            self.send_response(200)
            self._set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            telemetry = LOAD_BALANCER.get_telemetry()
            status = {
                "status": "online",
                "service": "Qinter LangGraph Agent Service",
                "load_balancer": telemetry,
                "modes_supported": ["AUTO_BALANCED", "NVIDIA_NIM", "VPS_LOCAL_LLM"],
                "features": ["conversational_chatgpt_comfort", "live_web_search", "document_intelligence"],
                "env_configured": {
                    "has_nim_key": bool(os.getenv("NVIDIA_NIM_API_KEY")),
                    "nim_model": os.getenv("NVIDIA_NIM_MODEL", "nvidia/nemotron-3.5-bolt-e4b"),
                    "ollama_host": os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434"),
                    "redis_url": os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
                }
            }
            self.wfile.write(json.dumps(status, indent=2).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path.startswith("/api/transcribe"):
            parsed = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(parsed.query)
            lang = qs.get("lang", ["fr"])[0]
            lang_map = {
                "fr": "fr-FR", "en": "en-US", "ar": "ar-SA", "es": "es-ES",
                "de": "de-DE", "it": "it-IT", "zh": "zh-CN", "ja": "ja-JP"
            }
            target_lang = lang_map.get(lang.lower(), lang)
            content_length = int(self.headers.get("Content-Length", 0) or 0)
            raw_audio = self.rfile.read(content_length)
            transcribed = transcribe_audio_bytes(raw_audio, target_lang)
            self._send_json(200, {"text": transcribed, "lang": target_lang})
            return

        if self.path.startswith("/api/auth/"):
            content_length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(min(content_length, 64 * 1024))
            try:
                body = json.loads(raw.decode("utf-8")) if raw else {}
            except Exception:
                body = {}
            ip = self.client_address[0]
            route = self.path
            if route == "/api/auth/signup":
                status, payload = auth_service.signup(body, ip)
            elif route == "/api/auth/signin":
                status, payload = auth_service.signin(body, ip)
            elif route == "/api/auth/google":
                status, payload = auth_service.google_signin(body, ip)
            elif route == "/api/auth/logout":
                status, payload = auth_service.logout(self.headers)
            else:
                status, payload = 404, {"error": "Unknown auth route"}
            self._send_json(status, payload)
            return

        if self.path == "/api/agent/chat":
            content_length = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_length)
            try:
                body = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                body = {}

            prompt = body.get("prompt", "")
            transcript = body.get("transcript", "")
            history = body.get("history", [])
            search_web = bool(body.get("search_web", False) or body.get("searchWeb", False))

            mode = body.get("mode", os.getenv("LOAD_BALANCING_MODE", "AUTO_BALANCED"))
            vps_host = body.get("vps_host", os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434"))
            vps_model = body.get("vps_model", os.getenv("OLLAMA_MODEL", "llama3.1:8b"))
            nim_key = body.get("nim_api_key", os.getenv("NVIDIA_NIM_API_KEY", ""))
            nim_model = body.get("nim_model", os.getenv("NVIDIA_NIM_MODEL", "nvidia/nemotron-3.5-bolt-e4b"))

            result = run_agent_workflow(
                prompt=prompt,
                transcript=transcript,
                history=history,
                search_web=search_web,
                mode=mode,
                vps_host=vps_host,
                vps_model=vps_model,
                nim_api_key=nim_key,
                nim_model=nim_model
            )

            try:
                self.send_response(200)
                self._set_cors_headers()
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.end_headers()
                self.wfile.write(json.dumps(result, ensure_ascii=False).encode("utf-8"))
            except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                pass
        else:
            self.send_response(404)
            self.end_headers()


def main():
    parser = argparse.ArgumentParser(description="Qinter LangGraph Agent Service")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on (default: 8000)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host binding (default: 0.0.0.0)")
    parser.add_argument("--test", action="store_true", help="Run a self-test of the LangGraph graph")
    args = parser.parse_args()

    if args.test:
        print("Running LangGraph Redis Load-Balancer Self-Test...")
        sample_transcript = "[00:15] Bienvenue dans la présentation sur Qinter.\n[01:05] Le système bascule automatiquement entre NVIDIA NIM et Ollama."
        
        test_res = run_agent_workflow(
            prompt="Comment fonctionne le load balancing ?",
            transcript=sample_transcript,
            mode="AUTO_BALANCED"
        )
        print("Self-test completed!")
        print(f"Backend Used: {test_res.get('backendUsed')}")
        print(f"Detected language: {test_res.get('detectedLanguage')}")
        print(f"Reply preview: {test_res.get('reply')[:200]}...")
        return

    server = ThreadedHTTPServer((args.host, args.port), AgentRequestHandler)
    print(f"=====================================================")
    print(f"  Qinter LangGraph AI Agent Server (Redis Load Balanced)")
    print(f"  Address:  http://{args.host}:{args.port}")
    print(f"  Endpoint: POST http://{args.host}:{args.port}/api/agent/chat")
    print(f"  Stats:    GET  http://{args.host}:{args.port}/api/agent/stats")
    print(f"  Health:   GET  http://{args.host}:{args.port}/health")
    print(f"  Tier 1:   NVIDIA NIM ({os.getenv('NVIDIA_NIM_MODEL', 'nvidia/nemotron-3.5-bolt-e4b')})")
    print(f"  Tier 2:   Ollama Local ({os.getenv('OLLAMA_MODEL', 'qwen2.5:3b')})")
    print(f"  Redis:    {os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')}")
    print(f"=====================================================")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server gracefully...")
        server.server_close()


if __name__ == "__main__":
    main()
