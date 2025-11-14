"""Utility helpers for loading and summarizing PerfMon CSV logs."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence

import pandas as pd

DEFAULT_CATEGORY_KEYWORDS: Mapping[str, Sequence[str]] = {
    "CPU": ("cpu", "processor", "% processor", "total processor"),
    "GPU": ("gpu", "nvidia", "graphics"),
    "Memory": ("memory", "commit", "pagefile", "pool"),
    "Disk": ("disk", "storage", "io"),
    "Network": ("network", "net", "ethernet", "throughput"),
}


@dataclass
class MetricStat:
    """Stores min/max/average for a single metric."""

    name: str
    min: float
    max: float
    avg: float

    def to_dict(self) -> Dict[str, float | str]:
        return {"name": self.name, "min": self.min, "max": self.max, "avg": self.avg}


class PerfmonAnalyzer:
    """Loads and summarizes PerfMon CSV files."""

    def __init__(
        self,
        csv_path: str | Path,
        *,
        category_keywords: Mapping[str, Iterable[str]] | None = None,
        encoding: str = "utf-8",
    ) -> None:
        self.csv_path = Path(csv_path)
        self.encoding = encoding
        self.category_keywords: Mapping[str, Iterable[str]] = (
            category_keywords if category_keywords is not None else DEFAULT_CATEGORY_KEYWORDS
        )
        self._dataframe: pd.DataFrame | None = None
        self._numeric_dataframe: pd.DataFrame | None = None
        self._metric_stats: List[MetricStat] | None = None
        self._category_stats: Dict[str, Dict[str, float]] | None = None

    def load(self) -> None:
        """Loads the CSV file into a pandas DataFrame."""

        if not self.csv_path.exists():
            raise FileNotFoundError(f"Could not find PerfMon log at {self.csv_path}")

        df = pd.read_csv(self.csv_path, encoding=self.encoding)
        if df.empty:
            raise ValueError("The provided PerfMon log is empty")

        timestamp_column = df.columns[0]
        df = df.rename(columns={timestamp_column: "Timestamp"})
        self._dataframe = df

        numeric_df = df.drop(columns=["Timestamp"], errors="ignore").apply(pd.to_numeric, errors="coerce")
        self._numeric_dataframe = numeric_df
        self._metric_stats = None
        self._category_stats = None

    @property
    def dataframe(self) -> pd.DataFrame:
        if self._dataframe is None:
            self.load()
        assert self._dataframe is not None
        return self._dataframe

    @property
    def numeric_dataframe(self) -> pd.DataFrame:
        if self._numeric_dataframe is None:
            self.load()
        assert self._numeric_dataframe is not None
        return self._numeric_dataframe

    def metric_stats(self) -> List[MetricStat]:
        if self._metric_stats is not None:
            return self._metric_stats

        stats: List[MetricStat] = []
        numeric_df = self.numeric_dataframe
        for column in numeric_df.columns:
            series = numeric_df[column].dropna()
            if series.empty:
                continue
            stats.append(
                MetricStat(
                    name=column,
                    min=float(series.min()),
                    max=float(series.max()),
                    avg=float(series.mean()),
                )
            )

        stats.sort(key=lambda stat: stat.name)
        self._metric_stats = stats
        return stats

    def _match_category(self, metric_name: str) -> str:
        metric_lower = metric_name.lower()
        for category, keywords in self.category_keywords.items():
            if any(keyword in metric_lower for keyword in keywords):
                return category
        return "Other"

    def category_stats(self) -> Dict[str, Dict[str, float]]:
        if self._category_stats is not None:
            return self._category_stats

        numeric_df = self.numeric_dataframe
        category_frames: Dict[str, List[str]] = {}
        for column in numeric_df.columns:
            category = self._match_category(column)
            category_frames.setdefault(category, []).append(column)

        category_stats: Dict[str, Dict[str, float]] = {}
        for category, columns in category_frames.items():
            subset = numeric_df[columns]
            series = subset.mean(axis=1, skipna=True)
            if series.dropna().empty:
                continue
            category_stats[category] = {
                "min": float(series.min()),
                "max": float(series.max()),
                "avg": float(series.mean()),
                "metrics": columns,
            }

        self._category_stats = category_stats
        return category_stats
