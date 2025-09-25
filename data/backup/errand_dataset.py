from __future__ import annotations
import pandas as pd
from dataclasses import dataclass, field
from typing import Optional
from ...app.services.services import DefaultServices

@dataclass
class ErrandDataset:
    """Encapsulates preloaded errands to be used for matching."""
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)

    @classmethod
    def from_db(cls, condition_sql: str, services: Optional[DefaultServices] = None) -> "ErrandDataset":
        """Load errands by a SQL WHERE condition using the existing Connector."""
        services = services or DefaultServices()
        conn = services.get_connector()
        df = conn._fetch_and_format_errand(condition_sql)
        if df is None:
            df = pd.DataFrame()
        return cls(df=df, services=services)

    def to_frame(self) -> pd.DataFrame:
        return self.df.copy()
