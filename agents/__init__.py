# agents/__init__.py
"""
Tableau Multi-Agent System — Agents Package
============================================
All agent implementations inherit from BaseAgent.
LLM routing is handled by LLMRouter (reads config/settings.yaml).
"""

from agents.base_agent import BaseAgent, AgentResult, AgentStatus, PhaseContext
from agents.llm_router import LLMRouter

__all__ = [
    "BaseAgent",
    "AgentResult",
    "AgentStatus",
    "PhaseContext",
    "LLMRouter",
]
