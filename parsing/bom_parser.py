"""BOM ingestion, validation, normalization, and flagging."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

import pandas as pd

MAX_ROWS = 500
MAX_FILE_BYTES = 5 * 1024 * 1024  # 5 MB

REQUIRED_COLUMNS = {"component", "material", "quantity", "spend_usd"}
OPTIONAL_COLUMNS = {"weight_kg", "supplier", "country_of_origin"}
ALL_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS

# Imperial-to-metric conversion factors for weight_kg column
_UNIT_PATTERN = re.compile(
    r"^\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*(lb|lbs|oz|g|kg)?\s*$",
    re.IGNORECASE,
)
_LB_TO_KG = 0.453592
_OZ_TO_KG = 0.0283495
_G_TO_KG = 0.001

# Anomaly threshold: flag values more than this multiple above/below the median
_ANOMALY_RATIO = 10.0


@dataclass
class BOMFlag:
    row_index: int  # 0-based index into ParsedBOM.rows
    field: str
    flag_type: str  # "missing" | "anomalous" | "duplicate" | "ambiguous" | "formatting_fixed"
    message: str
    severity: str  # "error" | "warning"


@dataclass
class BOMRow:
    row_index: int
    component: str | None
    material: str | None
    quantity: float | None
    spend_usd: float | None
    weight_kg: float | None
    supplier: str | None
    country_of_origin: str | None
    flags: list[BOMFlag] = field(default_factory=list)


@dataclass
class ParsedBOM:
    product_name: str
    rows: list[BOMRow]
    file_errors: list[str]  # fatal file-level errors that prevent processing

    @property
    def is_valid(self) -> bool:
        """True when no fatal file errors and at least one processable row."""
        return len(self.file_errors) == 0 and len(self.rows) > 0

    @property
    def flagged_row_indices(self) -> set[int]:
        return {r.row_index for r in self.rows if r.flags}

    @property
    def all_flags(self) -> list[BOMFlag]:
        return [f for r in self.rows for f in r.flags]


def parse_bom_csv(
    source: Union[str, Path, bytes, io.IOBase],
    product_name: str = "Unknown Product",
) -> ParsedBOM:
    """Parse a BOM CSV from a file path, bytes, or file-like object.

    Returns a ParsedBOM regardless of validity; check .is_valid and .file_errors.
    """
    raw_df, file_errors = _load_csv(source)
    if file_errors:
        return ParsedBOM(product_name=product_name, rows=[], file_errors=file_errors)

    column_errors = _validate_columns(raw_df)
    if column_errors:
        return ParsedBOM(product_name=product_name, rows=[], file_errors=column_errors)

    if len(raw_df) > MAX_ROWS:
        return ParsedBOM(
            product_name=product_name,
            rows=[],
            file_errors=[
                f"File has {len(raw_df)} rows; maximum supported is {MAX_ROWS}. "
                "Please split your BOM and upload in batches."
            ],
        )

    if len(raw_df) == 0:
        return ParsedBOM(
            product_name=product_name,
            rows=[],
            file_errors=["File is empty — no data rows found. Please upload a non-empty BOM."],
        )

    rows = _build_rows(raw_df)
    _flag_duplicates(rows)
    _flag_anomalous_numerics(rows)

    return ParsedBOM(product_name=product_name, rows=rows, file_errors=[])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_csv(source: Union[str, Path, bytes, io.IOBase]) -> tuple[pd.DataFrame, list[str]]:
    try:
        if isinstance(source, bytes):
            if len(source) > MAX_FILE_BYTES:
                return pd.DataFrame(), [
                    f"File exceeds 5 MB limit ({len(source) / 1e6:.1f} MB). "
                    "Please reduce the file size."
                ]
            source = io.BytesIO(source)
        elif isinstance(source, (str, Path)):
            path = Path(source)
            if path.stat().st_size > MAX_FILE_BYTES:
                return pd.DataFrame(), [
                    f"File exceeds 5 MB limit. Please reduce the file size."
                ]
        df = pd.read_csv(source, dtype=str, encoding="utf-8", skip_blank_lines=True)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df = df.dropna(how="all")
        return df, []
    except UnicodeDecodeError:
        return pd.DataFrame(), [
            "File could not be read — ensure it is UTF-8 encoded CSV."
        ]
    except Exception as exc:
        return pd.DataFrame(), [f"Unreadable file: {exc}. Please upload a valid CSV."]


def _validate_columns(df: pd.DataFrame) -> list[str]:
    present = set(df.columns)
    missing = REQUIRED_COLUMNS - present
    if missing:
        cols = ", ".join(sorted(missing))
        return [
            f"Missing required column(s): {cols}. "
            "Please add them and re-upload."
        ]
    return []


def _build_rows(df: pd.DataFrame) -> list[BOMRow]:
    rows: list[BOMRow] = []
    for idx, raw in df.iterrows():
        row_index = int(idx)  # type: ignore[arg-type]
        flags: list[BOMFlag] = []

        component = _clean_str(raw.get("component"))
        material = _clean_str(raw.get("material"))
        supplier = _clean_str(raw.get("supplier"))
        country = _clean_str(raw.get("country_of_origin"))

        quantity, q_flags = _parse_numeric_field(
            raw.get("quantity"), row_index, "quantity", flag_if_missing=False
        )
        flags.extend(q_flags)

        spend_usd, s_flags = _parse_numeric_field(raw.get("spend_usd"), row_index, "spend_usd")
        flags.extend(s_flags)

        weight_kg_raw = raw.get("weight_kg")
        weight_kg, w_flags = _parse_weight(weight_kg_raw, row_index)
        flags.extend(w_flags)

        if not component:
            flags.append(
                BOMFlag(
                    row_index=row_index,
                    field="component",
                    flag_type="missing",
                    message="Component name is missing — please review and fill in.",
                    severity="error",
                )
            )
        if not material:
            flags.append(
                BOMFlag(
                    row_index=row_index,
                    field="material",
                    flag_type="missing",
                    message="Material is missing — row cannot be matched to an emission factor.",
                    severity="error",
                )
            )

        rows.append(
            BOMRow(
                row_index=row_index,
                component=component,
                material=material,
                quantity=quantity,
                spend_usd=spend_usd,
                weight_kg=weight_kg,
                supplier=supplier,
                country_of_origin=country,
                flags=flags,
            )
        )
    return rows


def _clean_str(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s else None


def _parse_numeric_field(
    raw: object,
    row_index: int,
    field_name: str,
    flag_if_missing: bool = True,
) -> tuple[float | None, list[BOMFlag]]:
    """Parse a numeric field; return (value_or_None, flags)."""
    flags: list[BOMFlag] = []
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        if flag_if_missing:
            flags.append(
                BOMFlag(
                    row_index=row_index,
                    field=field_name,
                    flag_type="missing",
                    message=f"{field_name} is missing — row flagged for human review.",
                    severity="warning",
                )
            )
        return None, flags

    s = str(raw).strip()
    if not s:
        if flag_if_missing:
            flags.append(
                BOMFlag(
                    row_index=row_index,
                    field=field_name,
                    flag_type="missing",
                    message=f"{field_name} is missing — row flagged for human review.",
                    severity="warning",
                )
            )
        return None, flags

    try:
        value = float(s)
    except ValueError:
        flags.append(
            BOMFlag(
                row_index=row_index,
                field=field_name,
                flag_type="anomalous",
                message=f"{field_name} value '{s}' is not a valid number.",
                severity="error",
            )
        )
        return None, flags

    if value < 0:
        flags.append(
            BOMFlag(
                row_index=row_index,
                field=field_name,
                flag_type="anomalous",
                message=f"{field_name} is negative ({value}) — flagged for review.",
                severity="warning",
            )
        )
    elif value == 0:
        flags.append(
            BOMFlag(
                row_index=row_index,
                field=field_name,
                flag_type="anomalous",
                message=f"{field_name} is zero — flagged for review.",
                severity="warning",
            )
        )

    return value, flags


def _parse_weight(raw: object, row_index: int) -> tuple[float | None, list[BOMFlag]]:
    """Parse weight_kg, converting from imperial if needed."""
    flags: list[BOMFlag] = []
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None, flags

    s = str(raw).strip()
    if not s:
        return None, flags

    match = _UNIT_PATTERN.match(s)
    if not match:
        flags.append(
            BOMFlag(
                row_index=row_index,
                field="weight_kg",
                flag_type="anomalous",
                message=f"weight_kg value '{s}' could not be parsed.",
                severity="warning",
            )
        )
        return None, flags

    numeric, unit = float(match.group(1)), (match.group(2) or "").lower()

    if unit in ("lb", "lbs"):
        converted = numeric * _LB_TO_KG
        flags.append(
            BOMFlag(
                row_index=row_index,
                field="weight_kg",
                flag_type="formatting_fixed",
                message=f"weight_kg converted from {numeric} lb to {converted:.6f} kg.",
                severity="warning",
            )
        )
        return converted, flags

    if unit == "oz":
        converted = numeric * _OZ_TO_KG
        flags.append(
            BOMFlag(
                row_index=row_index,
                field="weight_kg",
                flag_type="formatting_fixed",
                message=f"weight_kg converted from {numeric} oz to {converted:.6f} kg.",
                severity="warning",
            )
        )
        return converted, flags

    if unit == "g":
        converted = numeric * _G_TO_KG
        flags.append(
            BOMFlag(
                row_index=row_index,
                field="weight_kg",
                flag_type="formatting_fixed",
                message=f"weight_kg converted from {numeric} g to {converted:.6f} kg.",
                severity="warning",
            )
        )
        return converted, flags

    if numeric < 0:
        flags.append(
            BOMFlag(
                row_index=row_index,
                field="weight_kg",
                flag_type="anomalous",
                message=f"weight_kg is negative ({numeric}) — flagged for review.",
                severity="warning",
            )
        )

    return numeric, flags


def _flag_duplicates(rows: list[BOMRow]) -> None:
    """Flag rows that appear to be duplicates of each other."""
    seen: dict[tuple, list[int]] = {}
    for row in rows:
        key = (
            (row.component or "").lower().strip(),
            (row.material or "").lower().strip(),
            round(row.weight_kg or 0, 4),
            (row.supplier or "").lower().strip(),
        )
        seen.setdefault(key, []).append(row.row_index)

    for key, indices in seen.items():
        if len(indices) > 1:
            for idx in indices:
                row = next(r for r in rows if r.row_index == idx)
                others = [i for i in indices if i != idx]
                row.flags.append(
                    BOMFlag(
                        row_index=idx,
                        field="row",
                        flag_type="duplicate",
                        message=(
                            f"This row appears to be a duplicate of row(s) "
                            f"{[o + 1 for o in others]} "
                            "(same component, material, weight, supplier). "
                            "Proceeding with calculation but flagging for review."
                        ),
                        severity="warning",
                    )
                )


def _flag_anomalous_numerics(rows: list[BOMRow]) -> None:
    """Flag numeric outliers using ratio-to-median: values >10x or <1/10th the median."""
    for col in ("quantity", "spend_usd"):
        values = [(r, getattr(r, col)) for r in rows if getattr(r, col) is not None and getattr(r, col) > 0]
        if len(values) < 2:
            continue
        nums = sorted(v for _, v in values)
        mid = len(nums) // 2
        median = (nums[mid] + nums[~mid]) / 2  # works for both even and odd length
        if median == 0:
            continue
        for row, val in values:
            ratio = val / median
            if ratio > _ANOMALY_RATIO or ratio < 1 / _ANOMALY_RATIO:
                already_flagged = any(
                    f.field == col and f.flag_type == "anomalous" for f in row.flags
                )
                if not already_flagged:
                    direction = "high" if ratio > _ANOMALY_RATIO else "low"
                    row.flags.append(
                        BOMFlag(
                            row_index=row.row_index,
                            field=col,
                            flag_type="anomalous",
                            message=(
                                f"{col} value {val} is unusually {direction} "
                                f"(median is {median}) — flagged for review."
                            ),
                            severity="warning",
                        )
                    )
