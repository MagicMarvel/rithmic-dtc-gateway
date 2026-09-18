#!/usr/bin/env python3
"""Replay persisted strike snapshots through the current GEX level rules.

This is deliberately independent of persisted call/put wall and trigger fields:
every level is recomputed from the raw strike rows without future information.
"""

from __future__ import annotations

import argparse
import collections
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

MINUTE_US = 60_000_000
SAMPLE_US = 60 * MINUTE_US
HORIZON_US = 60 * MINUTE_US
REACTION_US = 15 * MINUTE_US
MOMENTUM_LOOKBACK_US = 5 * MINUTE_US
WALL_CONFIRM_US = 2 * MINUTE_US
TOLERANCE = 1.0
REACTION_MOVE = 5.0
FAILURE_MOVE = 3.0
MOMENTUM_OVERRIDE = 5.0
WALL_RETAIN_FRACTION = 0.60


def finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def directionally_ahead(level: float, spot: float, upper: bool) -> bool:
    return level > spot + TOLERANCE if upper else level < spot - TOLERANCE


def trigger_values(rows: list[dict], field: str) -> list[tuple[float, float, float]]:
    values = []
    for row in rows:
        strike, magnitude, confidence = row.get("strike"), row.get(field), row.get("confidence", 0.0)
        if finite_number(strike) and finite_number(magnitude) and finite_number(confidence):
            values.append((float(strike), abs(float(magnitude)), float(confidence)))
    return values


def trigger_candidate(
    values: Iterable[tuple[float, float, float]],
    spot: float,
    upper: bool,
    strength_floor: float,
) -> float | None:
    eligible = [
        value
        for value in values
        if value[1] > 0.0
        and value[2] >= 50.0
        and directionally_ahead(value[0], spot, upper)
    ]
    if not eligible:
        return None
    strongest = max(value[1] for value in eligible)
    distance_scale = min(30.0, max(8.0, spot * 0.0015))
    qualified = [value for value in eligible if value[1] >= strongest * strength_floor]
    return max(
        qualified,
        key=lambda value: value[1]
        * math.exp(-abs(value[0] - spot) / distance_scale)
        * min(1.0, max(0.5, value[2] / 100.0)),
    )[0]


class StickyTrigger:
    def __init__(self, hold_us: int, retain_fraction: float) -> None:
        self.level: float | None = None
        self.release_after_publish = False
        self.selected_at_us = 0
        self.hold_us = hold_us
        self.retain_fraction = retain_fraction

    def update(
        self,
        candidate: float | None,
        values: list[tuple[float, float, float]],
        spot: float,
        upper: bool,
        as_of_us: int,
    ) -> float | None:
        if self.release_after_publish:
            self.level = candidate
            self.release_after_publish = False
            self.selected_at_us = as_of_us
            return self.level
        if self.level is None:
            self.level = candidate
            self.selected_at_us = as_of_us
            return self.level
        if as_of_us - self.selected_at_us >= self.hold_us:
            self.level = candidate
            self.selected_at_us = as_of_us
            return self.level
        eligible = [
            value
            for value in values
            if value[2] >= 50.0
            and value[1] > 0.0
            and directionally_ahead(value[0], spot, upper)
        ]
        if not directionally_ahead(self.level, spot, upper):
            self.release_after_publish = True
            return self.level
        strongest = max((value[1] for value in eligible), default=None)
        current = next(
            (value[1] for value in eligible if abs(value[0] - self.level) < 1e-9), None
        )
        if (
            strongest is not None
            and current is not None
            and current >= strongest * self.retain_fraction
        ):
            return self.level
        self.level = candidate
        self.selected_at_us = as_of_us
        return self.level


def wall_values(rows: list[dict], gex_field: str, position_field: str) -> list[tuple[float, float]]:
    values = []
    for row in rows:
        strike, gex, position = row.get("strike"), row.get(gex_field), row.get(position_field)
        if not (finite_number(strike) and finite_number(gex) and finite_number(position)):
            continue
        gex, position = abs(float(gex)), abs(float(position))
        if gex > 0.0 and position > 0.0:
            values.append((float(strike), math.sqrt(gex * position)))
    return values


def wall_candidate(values: list[tuple[float, float]], spot: float, upper: bool) -> float | None:
    eligible = [value for value in values if directionally_ahead(value[0], spot, upper)]
    return max(eligible, key=lambda value: value[1])[0] if eligible else None


class StickyWall:
    def __init__(self) -> None:
        self.level: float | None = None
        self.challenger: float | None = None
        self.challenger_since_us = 0
        self.release_after_publish = False

    def update(
        self,
        candidate: float | None,
        values: list[tuple[float, float]],
        spot: float,
        upper: bool,
        as_of_us: int,
    ) -> float | None:
        if self.release_after_publish:
            self.level = candidate
            self.challenger = None
            self.release_after_publish = False
            return self.level
        if self.level is None:
            self.level = candidate
            return self.level
        if not directionally_ahead(self.level, spot, upper):
            self.release_after_publish = True
            self.challenger = None
            return self.level
        eligible = [value for value in values if directionally_ahead(value[0], spot, upper)]
        if not eligible:
            return self.level
        strongest_level, strongest_strength = max(eligible, key=lambda value: value[1])
        current = next(
            (value[1] for value in eligible if abs(value[0] - self.level) < 1e-9), None
        )
        if abs(strongest_level - self.level) < 1e-9 or (
            current is not None and current >= strongest_strength * WALL_RETAIN_FRACTION
        ):
            self.challenger = None
            return self.level
        if self.challenger is None or abs(self.challenger - strongest_level) >= 1e-9:
            self.challenger = strongest_level
            self.challenger_since_us = as_of_us
            return self.level
        if as_of_us - self.challenger_since_us >= WALL_CONFIRM_US:
            self.level = strongest_level
            self.challenger = None
        return self.level


class StickyRegime:
    def __init__(self, deadband: float, confirm_us: int) -> None:
        self.deadband = deadband
        self.confirm_us = confirm_us
        self.current: str | None = None
        self.challenger: str | None = None
        self.challenger_since_us = 0

    def update(self, net_gex: float, gross_gex: float, as_of_us: int) -> str:
        balance = net_gex / gross_gex if gross_gex > 0.0 else 0.0
        if self.current is None:
            self.current = "long-gamma" if net_gex >= 0.0 else "short-gamma"
            return self.current
        desired = None
        if balance >= self.deadband:
            desired = "long-gamma"
        elif balance <= -self.deadband:
            desired = "short-gamma"
        if desired is None or desired == self.current:
            self.challenger = None
            return self.current
        if self.confirm_us == 0:
            self.current = desired
            self.challenger = None
            return self.current
        if self.challenger != desired:
            self.challenger = desired
            self.challenger_since_us = as_of_us
            return self.current
        if as_of_us - self.challenger_since_us >= self.confirm_us:
            self.current = desired
            self.challenger = None
        return self.current


@dataclass
class Point:
    as_of_us: int
    spot: float
    regime: str
    call_trigger: float | None
    put_trigger: float | None
    call_wall: float | None
    put_wall: float | None


def replay(
    path: Path,
    underlying: str,
    trigger_hold_minutes: int,
    trigger_strength_floor: float,
    trigger_retain_fraction: float,
    regime_deadband: float,
    regime_confirm_minutes: float,
) -> collections.OrderedDict[int, list[Point]]:
    sessions: collections.OrderedDict[int, list[Point]] = collections.OrderedDict()
    states: dict[
        int, tuple[StickyTrigger, StickyTrigger, StickyWall, StickyWall, StickyRegime]
    ] = {}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            try:
                snapshot = json.loads(line)
            except json.JSONDecodeError:
                continue
            if snapshot.get("underlying") != underlying or not finite_number(
                snapshot.get("underlying_price")
            ):
                continue
            rows = snapshot.get("strikes")
            session = snapshot.get("session_started_us")
            as_of_us = snapshot.get("as_of_us")
            if not isinstance(rows, list) or not isinstance(session, int) or not isinstance(as_of_us, int):
                continue
            spot = float(snapshot["underlying_price"])
            call_values = trigger_values(rows, "call_gex")
            put_values = trigger_values(rows, "put_gex")
            call_wall_values = wall_values(rows, "call_gex", "call_oi")
            put_wall_values = wall_values(rows, "put_gex", "put_oi")
            call_trigger, put_trigger, call_wall, put_wall, regime = states.setdefault(
                session,
                (
                    StickyTrigger(trigger_hold_minutes * MINUTE_US, trigger_retain_fraction),
                    StickyTrigger(trigger_hold_minutes * MINUTE_US, trigger_retain_fraction),
                    StickyWall(),
                    StickyWall(),
                    StickyRegime(
                        regime_deadband, int(regime_confirm_minutes * MINUTE_US)
                    ),
                ),
            )
            gross_gex = sum(
                abs(float(row.get("call_gex", 0.0) or 0.0))
                + abs(float(row.get("put_gex", 0.0) or 0.0))
                for row in rows
            )
            net_gex = snapshot.get("net_gex", 0.0)
            net_gex = float(net_gex) if finite_number(net_gex) else 0.0
            point = Point(
                as_of_us=as_of_us,
                spot=spot,
                regime=regime.update(net_gex, gross_gex, as_of_us),
                call_trigger=call_trigger.update(
                    trigger_candidate(call_values, spot, True, trigger_strength_floor),
                    call_values,
                    spot,
                    True,
                    as_of_us,
                ),
                put_trigger=put_trigger.update(
                    trigger_candidate(put_values, spot, False, trigger_strength_floor),
                    put_values,
                    spot,
                    False,
                    as_of_us,
                ),
                call_wall=call_wall.update(
                    wall_candidate(call_wall_values, spot, True),
                    call_wall_values,
                    spot,
                    True,
                    as_of_us,
                ),
                put_wall=put_wall.update(
                    wall_candidate(put_wall_values, spot, False),
                    put_wall_values,
                    spot,
                    False,
                    as_of_us,
                ),
            )
            sessions.setdefault(session, []).append(point)
    return sessions


FIELDS = (("call_trigger", True), ("put_trigger", False), ("call_wall", True), ("put_wall", False))


def blank_result() -> collections.Counter[str]:
    return collections.Counter()


def validate(points: list[Point]) -> dict[str, collections.Counter[str]]:
    results = {name: blank_result() for name, _ in FIELDS}
    if not points:
        return results
    through_us = points[-1].as_of_us
    last_sample_us = -(1 << 63)
    for index, signal in enumerate(points):
        if signal.as_of_us - last_sample_us < SAMPLE_US:
            continue
        last_sample_us = signal.as_of_us
        end_us = signal.as_of_us + HORIZON_US
        if through_us < end_us:
            continue
        end_index = index
        while end_index + 1 < len(points) and points[end_index + 1].as_of_us <= end_us:
            end_index += 1
        window = points[index : end_index + 1]
        structural_short = signal.regime == "short-gamma"
        for name, upper in FIELDS:
            level = getattr(signal, name)
            if level is None or not directionally_ahead(level, signal.spot, upper):
                continue
            result = results[name]
            result["completed_signals"] += 1
            event_index = None
            for position, (previous, current) in enumerate(zip(window, window[1:]), start=1):
                if structural_short:
                    touched = (
                        previous.spot < level <= current.spot
                        if upper
                        else previous.spot > level >= current.spot
                    )
                else:
                    touched = abs(current.spot - level) <= TOLERANCE
                if touched:
                    event_index = position
                    break
            superseded_index = None
            superseded_reason = None
            for position, current in enumerate(window[1:], start=1):
                current_level = getattr(current, name)
                if current.regime != signal.regime:
                    superseded_index = position
                    superseded_reason = "regime"
                    break
                if current_level is None or abs(current_level - level) > 1e-9:
                    superseded_index = position
                    superseded_reason = "level"
                    break
            superseded = superseded_index is not None and (
                event_index is None or superseded_index <= event_index
            )
            if superseded:
                result["superseded_before_touch"] += 1
                result["superseded_" + str(superseded_reason)] += 1
            else:
                result["actionable_signals"] += 1
            if event_index is None:
                continue
            result["touches"] += 1
            if not superseded:
                result["actionable_touches"] += 1
            absolute_event_index = index + event_index
            event = points[absolute_event_index]
            prior = next(
                (
                    current
                    for current in reversed(points[: absolute_event_index + 1])
                    if current.as_of_us <= event.as_of_us - MOMENTUM_LOOKBACK_US
                ),
                None,
            )
            momentum = None if prior is None else event.spot - prior.spot
            momentum_continuation = momentum is not None and (
                momentum >= MOMENTUM_OVERRIDE if upper else momentum <= -MOMENTUM_OVERRIDE
            )
            trend = structural_short or momentum_continuation
            outcome = None
            reaction_end = event.as_of_us + REACTION_US
            for current in points[absolute_event_index : end_index + 1]:
                if current.as_of_us > reaction_end:
                    break
                if trend:
                    effective = current.spot >= level + REACTION_MOVE if upper else current.spot <= level - REACTION_MOVE
                    failed = current.spot <= level - FAILURE_MOVE if upper else current.spot >= level + FAILURE_MOVE
                else:
                    effective = current.spot <= level - REACTION_MOVE if upper else current.spot >= level + REACTION_MOVE
                    failed = current.spot >= level + FAILURE_MOVE if upper else current.spot <= level - FAILURE_MOVE
                if effective:
                    outcome = True
                    break
                if failed:
                    outcome = False
                    break
            key = "effective" if outcome is True else "failed" if outcome is False else "pending_reactions"
            result[key] += 1
            if not superseded:
                result["actionable_" + key] += 1
    return results


def merge(results: Iterable[collections.Counter[str]]) -> collections.Counter[str]:
    total: collections.Counter[str] = collections.Counter()
    for result in results:
        total.update(result)
    return total


def with_rates(result: collections.Counter[str]) -> dict[str, float | int | None]:
    output = dict(result)
    actionable = result["actionable_signals"]
    decided = result["actionable_effective"] + result["actionable_failed"]
    output["actionable_touch_rate_pct"] = (
        result["actionable_touches"] * 100.0 / actionable if actionable else None
    )
    output["actionable_effective_rate_pct"] = (
        result["actionable_effective"] * 100.0 / decided if decided else None
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_path", type=Path)
    parser.add_argument("--underlying", default="ESU6")
    parser.add_argument("--session", type=int)
    parser.add_argument("--trigger-hold-minutes", type=int, default=60)
    parser.add_argument("--trigger-strength-floor", type=float, default=0.25)
    parser.add_argument("--trigger-retain-fraction", type=float)
    parser.add_argument("--regime-deadband", type=float, default=0.0)
    parser.add_argument("--regime-confirm-minutes", type=float, default=0.0)
    args = parser.parse_args()
    if args.trigger_hold_minutes <= 0:
        parser.error("--trigger-hold-minutes must be positive")
    if not 0.0 < args.trigger_strength_floor <= 1.0:
        parser.error("--trigger-strength-floor must be in (0, 1]")
    if args.trigger_retain_fraction is None:
        args.trigger_retain_fraction = args.trigger_strength_floor
    if not 0.0 < args.trigger_retain_fraction <= 1.0:
        parser.error("--trigger-retain-fraction must be in (0, 1]")
    if not 0.0 <= args.regime_deadband < 1.0:
        parser.error("--regime-deadband must be in [0, 1)")
    if args.regime_confirm_minutes < 0.0:
        parser.error("--regime-confirm-minutes must be non-negative")
    sessions = replay(
        args.snapshot_path,
        args.underlying,
        args.trigger_hold_minutes,
        args.trigger_strength_floor,
        args.trigger_retain_fraction,
        args.regime_deadband,
        args.regime_confirm_minutes,
    )
    selected = {
        session: points
        for session, points in sessions.items()
        if args.session is None or session == args.session
    }
    per_session = {session: validate(points) for session, points in selected.items()}
    aggregate = {
        name: merge(report[name] for report in per_session.values()) for name, _ in FIELDS
    }
    trigger = merge((aggregate["call_trigger"], aggregate["put_trigger"]))
    wall = merge((aggregate["call_wall"], aggregate["put_wall"]))
    output = {
        "underlying": args.underlying,
        "trigger_hold_minutes": args.trigger_hold_minutes,
        "trigger_strength_floor": args.trigger_strength_floor,
        "trigger_retain_fraction": args.trigger_retain_fraction,
        "regime_deadband": args.regime_deadband,
        "regime_confirm_minutes": args.regime_confirm_minutes,
        "sessions": len(selected),
        "completed_sessions": sum(
            any(result["completed_signals"] for result in report.values())
            for report in per_session.values()
        ),
        "levels": {name: with_rates(result) for name, result in aggregate.items()},
        "combined_trigger": with_rates(trigger),
        "combined_wall": with_rates(wall),
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
