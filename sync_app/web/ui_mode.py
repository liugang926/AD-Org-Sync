from __future__ import annotations

from dataclasses import dataclass
from typing import Any


BASIC_UI_MODE = "basic"
ADVANCED_UI_MODE = "advanced"


@dataclass(frozen=True, slots=True)
class UIModePresentation:
    """Presentation policy for one request.

    UI mode deliberately contains no role or capability information.  It may
    reduce information density, but it must never authorize an operation.
    """

    name: str
    audience: str
    density: str
    max_primary_columns: int
    show_internal_identifiers: bool
    show_provider_connector_details: bool
    show_field_codes: bool
    show_audit_evidence: bool
    show_bulk_tools: bool
    show_routing_configuration: bool
    show_system_guidance: bool
    high_risk_interaction: str

    @property
    def is_basic(self) -> bool:
        return self.name == BASIC_UI_MODE

    @property
    def is_advanced(self) -> bool:
        return self.name == ADVANCED_UI_MODE

    def choose(self, basic_value: Any, advanced_value: Any) -> Any:
        return advanced_value if self.is_advanced else basic_value


UI_MODE_PRESENTATIONS = {
    BASIC_UI_MODE: UIModePresentation(
        name=BASIC_UI_MODE,
        audience="System administrators",
        density="business",
        max_primary_columns=8,
        show_internal_identifiers=False,
        show_provider_connector_details=False,
        show_field_codes=False,
        show_audit_evidence=False,
        show_bulk_tools=False,
        show_routing_configuration=False,
        show_system_guidance=True,
        high_risk_interaction="wizard",
    ),
    ADVANCED_UI_MODE: UIModePresentation(
        name=ADVANCED_UI_MODE,
        audience="Implementation consultants and advanced administrators",
        density="technical",
        max_primary_columns=0,
        show_internal_identifiers=True,
        show_provider_connector_details=True,
        show_field_codes=True,
        show_audit_evidence=True,
        show_bulk_tools=True,
        show_routing_configuration=True,
        show_system_guidance=False,
        high_risk_interaction="confirmation",
    ),
}


def normalize_ui_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in UI_MODE_PRESENTATIONS else BASIC_UI_MODE


def get_ui_mode_presentation(value: Any) -> UIModePresentation:
    return UI_MODE_PRESENTATIONS[normalize_ui_mode(value)]
