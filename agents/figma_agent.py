"""
Figma Design Agent
==================
Phase 2-B of the pipeline (runs in parallel with MSTR Metric Mapper Agent).

Responsibilities:
1. PRIMARY: Call Figma REST API using file_key + token to extract:
   - Dashboard frame dimensions and positions (→ Tableau zone coordinates)
   - Color fills (→ Tableau color palette XML)
   - Typography specs (→ Tableau font formatting tokens)
2. FALLBACK: When no Figma API token is available, accept an image path
   (PNG/JPG) and use Claude Vision to extract the same information
3. Emit a DesignTokens dict consumed by the TWB Generator Agent
"""

from __future__ import annotations

import base64
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from agents.base_agent import AgentResult, BaseAgent


# Tableau Cloud supported font fallback map
FONT_FALLBACK: Dict[str, str] = {
    "Inter":       "Arial",
    "Roboto":      "Arial",
    "Montserrat":  "Trebuchet MS",
    "Poppins":     "Arial",
    "Lato":        "Arial",
    "Open Sans":   "Arial",
    "Source Sans": "Arial",
}


class FigmaDesignAgent(BaseAgent):
    """
    Extracts design tokens from a Figma file (API) or dashboard screenshot
    (Claude Vision). Outputs zones, colors, and typography tokens.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__("figma_design_agent", config)
        self.figma_token: str = os.environ.get("FIGMA_TOKEN", "")
        self.anthropic_api_key: str = os.environ.get("ANTHROPIC_API_KEY", "")
        self.llm_model: str = config.get("llm_model", "claude-opus-4-6")

    # ──────────────────────────────────────────
    # BaseAgent interface
    # ──────────────────────────────────────────

    def validate_input(self, state: Dict[str, Any]) -> List[str]:
        errors: List[str] = []
        spec = state.get("project_spec", {})
        has_figma_id = bool(spec.get("figma_file_id"))
        has_figma_token = bool(self.figma_token)
        has_image = bool(state.get("figma_image_path"))
        has_anthropic = bool(self.anthropic_api_key)

        if not has_figma_id and not has_image:
            errors.append(
                "Neither figma_file_id (in project_config.csv) nor "
                "figma_image_path (in state) is provided. "
                "At least one is required."
            )
        if has_figma_id and not has_figma_token and not has_image:
            # Soft: will fall back to vision if image provided later
            errors.append(
                "figma_file_id provided but FIGMA_TOKEN env var not set. "
                "Provide a dashboard screenshot via state['figma_image_path'] "
                "or set FIGMA_TOKEN."
            )
        if (not has_figma_token) and has_image and not has_anthropic:
            errors.append(
                "Vision fallback requires ANTHROPIC_API_KEY — not set."
            )
        return errors

    def run(self, state: Dict[str, Any]) -> AgentResult:
        spec: Dict[str, Any] = state["project_spec"]
        figma_file_id: str = spec.get("figma_file_id", "")
        image_path: Optional[str] = state.get("figma_image_path")
        warnings: List[str] = []

        design_tokens: Optional[Dict[str, Any]] = None

        # ── Attempt Figma API first ──
        if figma_file_id and self.figma_token:
            self.logger.info("figma_api_extraction_start", file_id=figma_file_id)
            design_tokens, api_errors = self._extract_via_api(figma_file_id)
            if api_errors:
                warnings.extend(api_errors)
                design_tokens = None  # force fallback

        # ── Fallback: Claude Vision ──
        if design_tokens is None:
            if image_path:
                self.logger.info(
                    "figma_vision_fallback_start", image_path=image_path
                )
                warnings.append(
                    "Figma API unavailable — using Claude Vision on provided image."
                )
                design_tokens, vision_errors = self._extract_via_vision(image_path)
                if vision_errors:
                    warnings.extend(vision_errors)
            else:
                # No Figma API AND no image → use sensible defaults
                warnings.append(
                    "No Figma source available — using default 2x2 grid layout "
                    "and Tableau default color palette."
                )
                design_tokens = self._default_design_tokens(
                    spec.get("dashboard_requirements", [])
                )

        # ── Post-process tokens ──
        design_tokens = self._normalise_tokens(design_tokens or {})

        status = "warning" if warnings else "success"
        return AgentResult(
            agent_id=self.agent_id,
            phase="figma_design",
            status=status,
            output={"design_tokens": design_tokens},
            warnings=warnings,
        )

    # ──────────────────────────────────────────
    # Figma REST API extraction
    # ──────────────────────────────────────────

    def _extract_via_api(
        self, file_key: str
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        errors: List[str] = []
        try:
            resp = requests.get(
                f"https://api.figma.com/v1/files/{file_key}",
                headers={"X-Figma-Token": self.figma_token},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            return None, [f"Figma API request failed: {exc}"]

        styles_map: Dict[str, Any] = data.get("styles", {})
        colors: Dict[str, Any] = {}
        text_styles: List[Dict[str, Any]] = []
        layouts: List[Dict[str, Any]] = []

        def traverse(node: Dict[str, Any]) -> None:
            # Colors from fills
            for fill in node.get("fills", []):
                if fill.get("type") == "SOLID" and fill.get("visible", True):
                    c = fill["color"]
                    hex_val = (
                        f"#{int(c['r'] * 255):02X}"
                        f"{int(c['g'] * 255):02X}"
                        f"{int(c['b'] * 255):02X}"
                    )
                    style_ref = node.get("styles", {}).get("fill", "")
                    name = styles_map.get(style_ref, {}).get("name", "")
                    colors[hex_val] = {"hex": hex_val, "style_name": name}

            # Text styles
            if node.get("type") == "TEXT":
                s = node.get("style", {})
                raw_font = s.get("fontFamily", "Arial")
                text_styles.append(
                    {
                        "font_family": FONT_FALLBACK.get(raw_font, raw_font),
                        "font_size": s.get("fontSize", 12),
                        "font_weight": s.get("fontWeight", 400),
                        "name": node.get("name"),
                    }
                )

            # Layout frames
            if node.get("type") == "FRAME":
                bb = node.get("absoluteBoundingBox", {})
                layouts.append(
                    {
                        "name": node.get("name"),
                        "node_id": node.get("id"),
                        "x": bb.get("x", 0),
                        "y": bb.get("y", 0),
                        "width": bb.get("width", 0),
                        "height": bb.get("height", 0),
                        "layout_mode": node.get("layoutMode", "NONE"),
                    }
                )

            for child in node.get("children", []):
                traverse(child)

        traverse(data["document"])
        self.logger.info(
            "figma_api_extraction_complete",
            colors=len(colors),
            layouts=len(layouts),
        )
        return {
            "source": "figma_api",
            "colors": list(colors.values()),
            "text_styles": text_styles,
            "layouts": layouts,
        }, errors

    # ──────────────────────────────────────────
    # Claude Vision fallback
    # ──────────────────────────────────────────

    def _extract_via_vision(
        self, image_path: str
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        errors: List[str] = []
        path = Path(image_path)
        if not path.exists():
            return None, [f"Image file not found: {image_path}"]

        suffix = path.suffix.lower()
        media_type_map = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}
        media_type = media_type_map.get(suffix, "image/png")

        with path.open("rb") as fh:
            image_b64 = base64.standard_b64encode(fh.read()).decode()

        vision_prompt = (
            "Analyse this Tableau dashboard design image. "
            "Return ONLY a valid JSON object (no markdown, no explanation) with:\n"
            '{\n'
            '  "colors": [{"hex": "#RRGGBB", "style_name": "label"}],\n'
            '  "text_styles": [{"font_family": "Arial", "font_size": 12, '
            '"font_weight": 400, "name": "heading"}],\n'
            '  "layouts": [\n'
            '    {"name": "Dashboard", "x": 0, "y": 0, "width": 1366, "height": 768, '
            '"layout_mode": "NONE"},\n'
            '    {"name": "Chart1", "x": 0, "y": 0, "width": 683, "height": 384, '
            '"layout_mode": "NONE"}\n'
            "  ]\n"
            "}\n\n"
            "The first layout entry must be the full dashboard frame. "
            "Each subsequent entry is a chart/KPI zone. "
            "Estimate x, y, width, height in pixels from the image dimensions."
        )

        try:
            from anthropic import Anthropic
            import json

            client = Anthropic(api_key=self.anthropic_api_key)
            message = client.messages.create(
                model=self.llm_model,
                max_tokens=2048,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": image_b64,
                                },
                            },
                            {"type": "text", "text": vision_prompt},
                        ],
                    }
                ],
            )
            raw = message.content[0].text.strip()
            # Strip any accidental markdown fences
            raw = raw.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(raw)
            parsed["source"] = "claude_vision"
            return parsed, errors

        except Exception as exc:  # noqa: BLE001
            errors.append(f"Claude Vision extraction failed: {exc}")
            return None, errors

    # ──────────────────────────────────────────
    # Default tokens (no Figma source)
    # ──────────────────────────────────────────

    def _default_design_tokens(
        self, dashboard_requirements: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Generate a sensible 2×2 grid layout from dashboard_requirements
        when no Figma source is available.
        """
        worksheet_views = [
            r for r in dashboard_requirements
            if r.get("view_type", "").lower() == "worksheet"
        ]
        count = len(worksheet_views) or 4
        cols = 2
        rows = (count + 1) // 2
        w, h = 1366, 768
        cell_w = w // cols
        cell_h = h // rows

        layouts: List[Dict[str, Any]] = [
            {
                "name": "Dashboard",
                "x": 0, "y": 0,
                "width": w, "height": h,
                "layout_mode": "NONE",
            }
        ]
        for i, view in enumerate(worksheet_views):
            col_idx = i % cols
            row_idx = i // cols
            layouts.append(
                {
                    "name": view.get("view_name", f"Sheet{i+1}"),
                    "x": col_idx * cell_w,
                    "y": row_idx * cell_h,
                    "width": cell_w,
                    "height": cell_h,
                    "layout_mode": "NONE",
                }
            )

        return {
            "source": "default_grid",
            "colors": [
                {"hex": "#1F4E79", "style_name": "Primary Blue"},
                {"hex": "#2E75B6", "style_name": "Accent Blue"},
                {"hex": "#F2F2F2", "style_name": "Background"},
            ],
            "text_styles": [
                {"font_family": "Arial", "font_size": 18, "font_weight": 700, "name": "Title"},
                {"font_family": "Arial", "font_size": 12, "font_weight": 400, "name": "Body"},
            ],
            "layouts": layouts,
        }

    # ──────────────────────────────────────────
    # Token normalisation
    # ──────────────────────────────────────────

    def _normalise_tokens(self, tokens: Dict[str, Any]) -> Dict[str, Any]:
        """Apply font fallback map and ensure hex colors are uppercase."""
        for ts in tokens.get("text_styles", []):
            raw = ts.get("font_family", "Arial")
            ts["font_family"] = FONT_FALLBACK.get(raw, raw)

        for color in tokens.get("colors", []):
            if "hex" in color:
                color["hex"] = color["hex"].upper()

        return tokens
