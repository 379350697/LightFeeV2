"""Chillybot-related config fields that must be rejected with migration errors."""

CHILLYBOT_FIELDS = frozenset(
    {
        "opportunity_source.chillybot_first",
        "chillybot_api_base",
        "chillybot_timeout_ms",
        "sidecar_chillybot_mode",
        "chillybot_mode",
        "opportunity_source.chillybot_via_feedgrab",
        "feedgrab",
    }
)

REMOVED_FIELD_MESSAGES: dict[str, str] = {
    "opportunity_source": (
        "removed Chillybot config field: runtime.opportunity_source. "
        "Python LightFee uses exchange-native sources only. "
        "Remove this field or set opportunity_input_mode = 'sidecar_backed'."
    ),
    "chillybot_api_base": (
        "removed Chillybot config field: runtime.chillybot_api_base. "
        "Python LightFee does not connect to Chillybot."
    ),
    "chillybot_timeout_ms": (
        "removed Chillybot config field: runtime.chillybot_timeout_ms. "
        "Python LightFee does not connect to Chillybot."
    ),
    "sidecar_chillybot_mode": (
        "removed Chillybot config field: runtime.sidecar_chillybot_mode. "
        "The Python sidecar uses exchange-native data only."
    ),
}

# Acceptable opportunity_input_mode values for the Python runtime
VALID_OPPORTUNITY_INPUT_MODES = frozenset({"sidecar_backed", "coarse_sidecar"})
