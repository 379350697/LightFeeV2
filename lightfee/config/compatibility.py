"""Chillybot field rejection and legacy config alias mapping."""

# Fields whose presence must trigger a migration error
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

# Non-Chillybot legacy field aliases preserved for backward compatibility.
# Keys are the old TOML key names; values are the canonical Python config attribute.
LEGACY_FIELD_ALIASES: dict[str, str] = {
    # Strategy legacy aliases
    "min_funding_edge": "min_funding_edge_bps",
    "funding_edge_floor": "min_funding_edge_bps",
    "expected_edge_floor": "min_expected_edge_bps",
    "worst_case_edge_floor": "min_worst_case_edge_bps",
    "entry_notional_cap": "entry_notional_cap_quote",
    "min_entry_notional": "min_entry_leg_notional_quote",
    "max_concurrent": "max_concurrent_positions",
    "single_venue_exposure_cap": "max_single_venue_exposure_quote",
    "symbol_exposure_cap": "max_symbol_exposure_quote",
    "global_net_exposure_cap": "max_global_net_exposure_quote",
    # Runtime legacy aliases
    "tick_interval_ms": "poll_interval_ms",
    "snapshot_path": "sidecar_snapshot_path",
    "snapshot_max_age_ms": "sidecar_snapshot_max_age_ms",
    "sidecar_interval_ms": "sidecar_refresh_ms",
    # Persistence legacy aliases
    "journal_path": "event_log_path",
    "state_path": "snapshot_path",
    # Risk legacy aliases
    "stop_loss_quote": "net_stop_loss_quote",
    "profit_target_quote": "profit_take_quote",
}

# Acceptable opportunity_input_mode values for the Python runtime (CONFIG-003)
# V1 parity: direct_market, coarse_sidecar, sidecar_scan, disabled, non_parity
VALID_OPPORTUNITY_INPUT_MODES = frozenset({
    "direct_market",
    "coarse_sidecar",
    "sidecar_backed",  # alias for coarse_sidecar (V1 compat)
    "sidecar_scan",
    "direct_market_enriched",  # V1: enriched provider with hints and transfer resolution
    "disabled",
    "non_parity",
})


def apply_legacy_aliases(raw: dict) -> dict:
    """Transparently rewrite old field names to canonical names in a parsed TOML dict.

    Returns a new dict with aliases resolved. Does NOT modify the input.
    """
    result: dict = {}
    for key, value in raw.items():
        canonical = LEGACY_FIELD_ALIASES.get(key, key)
        if isinstance(value, dict):
            result[canonical] = apply_legacy_aliases(value)
        else:
            result[canonical] = value
    return result
