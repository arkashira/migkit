"""Stored code conversion and rule generalization integration (Row 628: RCH20).

Wires conversion engines with narrow scoped rewrite rules, shadow application,
and decide ladders.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import decide, rule_generalization
from .storedcode import convert as _storedcode_convert


def convert_routine_with_generalization(
    routine: rule_generalization.RoutineDefinition,
    engine: Optional[rule_generalization.RuleGeneralizationEngine] = None,
    rules: Optional[Sequence[rule_generalization.LearnedRewriteRule]] = None,
    context_info: Optional[Dict[str, Any]] = None,
) -> rule_generalization.RewriteResult:
    """Converts a routine applying learned rewrite rules gated by narrow scoping and proof obligations."""
    eng = engine or rule_generalization.RuleGeneralizationEngine()
    return eng.apply_rules(routine, rules=rules, context_info=context_info)


def evaluate_routine_conversion(facts: Dict[str, Any]) -> decide.Choice:
    """Climbs rule_generalization_rungs decide ladder for routine conversion."""
    return rule_generalization.decide_rule_generalization(facts)


translator = _storedcode_convert.translator
rules_convert = _storedcode_convert.rules_convert
