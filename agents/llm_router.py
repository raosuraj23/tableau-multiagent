# agents/llm_router.py
"""
LLMRouter — Per-Agent LLM Dispatcher
======================================

Reads config/settings.yaml to determine which LLM (Claude or Gemini)
handles each agent, then returns the correctly configured LangChain
chat model instance.

Architecture:
    - Claude  (claude-sonnet-4-20250514) → XML generation, technical tasks
    - Gemini  (gemini-2.5-flash)         → semantic mapping, MSTR conversion

Usage:
    router = LLMRouter()

    # Get the LLM for a specific agent
    llm = router.get_llm("conversion_agent")    # → Gemini
    llm = router.get_llm("semantic_agent")       # → Claude

    # Direct invoke
    response = router.invoke("conversion_agent", "Translate this metric...")

    # Structured output (Claude only — for XML generation)
    structured_llm = router.get_structured_llm("metric_agent", MyPydanticModel)

    # Check which model an agent uses
    router.get_model_name("tableau_model_agent")  # → "claude-sonnet-4-20250514"
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

import yaml
import structlog
from dotenv import load_dotenv
from pydantic import BaseModel

# Load .env early so API keys are available
load_dotenv()

logger = structlog.get_logger().bind(component="llm_router")

# ── LangChain imports ─────────────────────────────────────────────────────────

try:
    from langchain_anthropic import ChatAnthropic
    _anthropic_available = True
except ImportError:
    _anthropic_available = False
    logger.warning("langchain_anthropic not installed — Claude unavailable")

try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    _google_available = True
except ImportError:
    _google_available = False
    logger.warning("langchain_google_genai not installed — Gemini unavailable")

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel


# ── Config path ───────────────────────────────────────────────────────────────

_PROJECT_ROOT = Path(__file__).parent.parent
_SETTINGS_PATH = _PROJECT_ROOT / "config" / "settings.yaml"
_LLM_CONFIG_PATH = _PROJECT_ROOT / "config" / "llm_config.yaml"


# ── LLMRouter ─────────────────────────────────────────────────────────────────

class LLMRouter:
    """
    Reads config/settings.yaml and returns the correct LangChain model
    instance for each agent based on the llm_routing table.

    Model instances are cached — one Claude instance, one Gemini instance.
    Re-creating LangChain models is expensive; caching avoids that cost.
    """

    def __init__(
        self,
        settings_path: Optional[Path] = None,
        llm_config_path: Optional[Path] = None,
    ) -> None:
        self._settings_path   = settings_path or _SETTINGS_PATH
        self._llm_config_path = llm_config_path or _LLM_CONFIG_PATH

        self._settings:   Dict[str, Any] = {}
        self._llm_config: Dict[str, Any] = {}
        self._cache:      Dict[str, BaseChatModel] = {}  # "claude" | "gemini" → model

        self._load_config()
        logger.info("llm_router_initialized",
                    routing_table=self._settings.get("llm_routing", {}))

    # ── Config loading ─────────────────────────────────────────────────────

    def _load_config(self) -> None:
        """Load settings.yaml and llm_config.yaml."""
        if not self._settings_path.exists():
            raise FileNotFoundError(
                f"settings.yaml not found at {self._settings_path}. "
                "Run from project root or pass settings_path explicitly."
            )
        with open(self._settings_path, "r", encoding="utf-8") as f:
            self._settings = yaml.safe_load(f)

        if self._llm_config_path.exists():
            with open(self._llm_config_path, "r", encoding="utf-8") as f:
                self._llm_config = yaml.safe_load(f) or {}

    # ── Model factory ──────────────────────────────────────────────────────

    def _build_claude(self) -> "ChatAnthropic":
        """Build and cache the Claude LangChain model instance."""
        if not _anthropic_available:
            raise ImportError("langchain-anthropic is not installed")

        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise EnvironmentError(
                "ANTHROPIC_API_KEY not set. Add it to your .env file."
            )

        cfg = self._settings.get("llm_models", {}).get("claude", {})
        model = ChatAnthropic(
            model=cfg.get("model_id", "claude-sonnet-4-20250514"),
            temperature=cfg.get("temperature", 0),
            max_tokens=cfg.get("max_tokens", 8192),
            timeout=cfg.get("timeout_seconds", 60),
            api_key=api_key,
        )
        logger.info("claude_model_built",
                    model_id=cfg.get("model_id", "claude-sonnet-4-20250514"))
        return model

    def _build_gemini(self) -> "ChatGoogleGenerativeAI":
        """Build and cache the Gemini LangChain model instance."""
        if not _google_available:
            raise ImportError("langchain-google-genai is not installed")

        api_key = os.getenv("GOOGLE_API_KEY", "")
        if not api_key:
            raise EnvironmentError(
                "GOOGLE_API_KEY not set. Add it to your .env file."
            )

        cfg = self._settings.get("llm_models", {}).get("gemini", {})
        model_id = cfg.get("model_id", "gemini-2.0-flash")

        # Auto-discover if set to "auto" or if the configured model is unavailable
        if model_id == "auto":
            model_id = self._discover_gemini_model(api_key)

        model = ChatGoogleGenerativeAI(
            model=model_id,
            temperature=cfg.get("temperature", 0.1),
            max_output_tokens=cfg.get("max_tokens", 8192),
            google_api_key=api_key,
        )
        logger.info("gemini_model_built", model_id=model_id)
        return model

    def _discover_gemini_model(self, api_key: str) -> str:
        """
        Dynamically discover the best available Gemini model for this API key.
        Tries configured fallbacks in order from settings.yaml.
        """
        try:
            from google import genai as google_genai
            client = google_genai.Client(api_key=api_key)
            available = []
            for m in client.models.list():
                name    = (m.name or "").replace("models/", "")
                actions = m.supported_actions or []
                if "generateContent" in actions and "gemini" in name.lower():
                    available.append(name)

            cfg       = self._settings.get("llm_models", {}).get("gemini", {})
            fallbacks = cfg.get("model_id_fallbacks", [
                "gemini-2.0-flash", "gemini-2.0-flash-lite", "gemini-1.5-flash"
            ])
            for preferred in fallbacks:
                if preferred in available:
                    logger.info("gemini_model_discovered", model_id=preferred,
                                all_available=available[:6])
                    return preferred

            # Nothing matched — use first available
            if available:
                logger.warning("gemini_fallback_model_used",
                               model_id=available[0])
                return available[0]

        except Exception as e:
            logger.warning("gemini_model_discovery_failed", error=str(e))

        # Hard fallback
        return "gemini-2.0-flash"

    # ── Public API ─────────────────────────────────────────────────────────

    def get_llm(self, agent_id: str) -> BaseChatModel:
        """
        Get the LangChain model instance for a given agent_id.

        The model is determined by llm_routing in config/settings.yaml.
        Instances are cached — multiple calls return the same object.

        Args:
            agent_id: The agent identifier (e.g. "conversion_agent")

        Returns:
            Configured LangChain BaseChatModel (Claude or Gemini)
        """
        provider = self._get_provider(agent_id)

        if provider not in self._cache:
            if provider == "claude":
                self._cache["claude"] = self._build_claude()
            elif provider == "gemini":
                self._cache["gemini"] = self._build_gemini()
            else:
                raise ValueError(
                    f"Unknown LLM provider '{provider}' for agent '{agent_id}'. "
                    "Must be 'claude' or 'gemini'."
                )

        return self._cache[provider]

    def get_model_name(self, agent_id: str) -> str:
        """Return the model ID string for a given agent (for logging/metadata)."""
        provider = self._get_provider(agent_id)
        cfg      = self._settings.get("llm_models", {}).get(provider, {})
        return cfg.get("model_id", f"unknown-{provider}")

    def get_provider(self, agent_id: str) -> str:
        """Return 'claude' or 'gemini' for a given agent_id."""
        return self._get_provider(agent_id)

    def get_structured_llm(
        self,
        agent_id: str,
        schema: Type[BaseModel],
    ) -> Any:
        """
        Get a structured-output LLM for a given agent.
        Uses LangChain's .with_structured_output() — guarantees schema compliance.

        Best used with Claude (more reliable structured output for complex schemas).

        Args:
            agent_id: The agent identifier
            schema:   Pydantic model class defining the expected output shape

        Returns:
            LangChain runnable that returns validated Pydantic model instances
        """
        llm = self.get_llm(agent_id)
        return llm.with_structured_output(schema)

    def invoke(
        self,
        agent_id: str,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_retries: int = 3,
    ) -> str:
        """
        Invoke the LLM for a given agent and return the response text.

        Handles retries with exponential backoff on transient failures.
        Uses the system prompt from llm_config.yaml if not provided explicitly.

        Args:
            agent_id:      The agent identifier
            prompt:        The user message to send
            system_prompt: Override the default system prompt from config
            max_retries:   Number of retry attempts on failure

        Returns:
            Response text string

        Raises:
            RuntimeError: If all retries are exhausted
        """
        llm = self.get_llm(agent_id)
        sys_prompt = system_prompt or self._get_system_prompt(agent_id)

        messages: List[Any] = []
        if sys_prompt:
            messages.append(SystemMessage(content=sys_prompt))
        messages.append(HumanMessage(content=prompt))

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                response = llm.invoke(messages)
                text = response.content if hasattr(response, "content") else str(response)
                logger.info(
                    "llm_invoked",
                    agent=agent_id,
                    provider=self._get_provider(agent_id),
                    attempt=attempt,
                    response_chars=len(text),
                )
                return text

            except Exception as e:
                last_exc = e
                wait = 2 ** attempt  # exponential: 2s, 4s, 8s
                logger.warning(
                    "llm_invoke_retry",
                    agent=agent_id,
                    attempt=attempt,
                    max_retries=max_retries,
                    wait_seconds=wait,
                    error=str(e),
                )
                if attempt < max_retries:
                    time.sleep(wait)

        raise RuntimeError(
            f"LLM invoke failed for agent '{agent_id}' after {max_retries} attempts. "
            f"Last error: {last_exc}"
        )

    def invoke_json(
        self,
        agent_id: str,
        prompt: str,
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        Invoke the LLM expecting a JSON response.
        Prepends JSON-only instruction to system prompt to reduce markdown fences.

        Returns raw text — caller is responsible for parsing.
        """
        json_instruction = (
            "You must respond with valid JSON only. "
            "No preamble, no explanation, no markdown code blocks. "
            "Start your response with { or [ and end with } or ]."
        )
        full_sys = (
            f"{json_instruction}\n\n{system_prompt}"
            if system_prompt
            else json_instruction
        )
        return self.invoke(agent_id, prompt, system_prompt=full_sys)

    def routing_table(self) -> Dict[str, str]:
        """Return the full agent → provider routing table for inspection."""
        return dict(self._settings.get("llm_routing", {}))

    def status(self) -> Dict[str, Any]:
        """Return router status including which models are cached."""
        return {
            "routing_table":   self.routing_table(),
            "cached_providers": list(self._cache.keys()),
            "anthropic_available": _anthropic_available,
            "google_available":    _google_available,
            "settings_path":   str(self._settings_path),
        }

    # ── Private helpers ────────────────────────────────────────────────────

    def _get_provider(self, agent_id: str) -> str:
        """Look up provider in llm_routing. Defaults to 'claude' if not found."""
        routing = self._settings.get("llm_routing", {})
        provider = routing.get(agent_id, "claude")
        if provider not in ("claude", "gemini"):
            logger.warning("unknown_provider_defaulting_to_claude",
                           agent_id=agent_id, provider=provider)
            return "claude"
        return provider

    def _get_system_prompt(self, agent_id: str) -> Optional[str]:
        """Get the system prompt for an agent from llm_config.yaml."""
        agent_cfg = self._llm_config.get(agent_id, {})
        return agent_cfg.get("system_prompt")
