from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
from .preprocess import PreProcess
from ..services.services import DefaultServices
from pandas.api.types import is_datetime64tz_dtype

@dataclass
class CategoryConnect:
    df: pd.DataFrame
    services: DefaultServices = field(default_factory=DefaultServices)

    def __post_init__(self):
        self.connector = self.services.get_connector()
        self.classifier = self.services.get_classifier()
        
    def single_connect(self, emails: pd.DataFrame, errands: pd.DataFrame) -> pd.DataFrame:
        if errands is None or errands.empty:
            return emails
        
        df = emails.copy()
        unmatched_mask = df['errandId'].apply(lambda x: not x)
        if unmatched_mask.any():
            sub = df.loc[unmatched_mask].copy() 
            applied = sub.apply(lambda row: self.connector.find_match_for_single_email(row, errands), axis=1)
            applied = pd.DataFrame.from_records(applied, index=sub.index)

            hit_mask = applied.get('errand_matched', pd.Series(False, index=applied.index)) == True
            hit_idx = applied.index[hit_mask]
            if len(hit_idx) > 0:
                cols = [c for c in applied.columns if c not in ('errand_matched', 'errandId')]
                for c in cols:
                    if c in df.columns and is_datetime64tz_dtype(df[c]):
                        s = pd.to_datetime(applied.loc[hit_idx, c], errors='coerce', utc=True)
                        s = s.dt.tz_convert(str(df[c].dtype.tz))
                        df.loc[hit_idx, c] = df.loc[hit_idx, c].combine_first(s)
                    else:
                        df.loc[hit_idx, c] = df.loc[hit_idx, c].combine_first(applied.loc[hit_idx, c]) \
                                                if c in df.columns else applied.loc[hit_idx, c]
                if 'errandId' in applied.columns:
                    def _merge_ids(a, b):
                        a = a if isinstance(a, list) else ([] if pd.isna(a) else [a])
                        b = b if isinstance(b, list) else ([] if pd.isna(b) else [b])
                        seen, out = set(), []
                        for x in a + b:
                            if x not in seen:
                                seen.add(x)
                                out.append(x)
                        return out
                    df.loc[hit_idx, 'errandId'] = [
                        _merge_ids(df.at[i, 'errandId'], applied.at[i, 'errandId']) for i in hit_idx
                    ]
            
        return df
    
    def connect_with_time_windows(self) -> pd.DataFrame:
        """Connect emails with errands.
        - If an ErrandDataset is provided, use its preloaded DataFrame.
        - Otherwise, fall back to the existing DB-backed method that fetches.
        """
        df = self.df.copy()
        windows = getattr(self.connector, 'errand_query_condition', [
            "er.\"createdAt\" >= NOW() - INTERVAL '15 day'",
            "er.\"createdAt\" >= NOW() - INTERVAL '3 month' AND er.\"createdAt\" < NOW() - INTERVAL '15 day'",
        ])

        for condi in windows:
            unmatched_mask = self.df['errandId'].apply(lambda x: not x)
            if not unmatched_mask.any():
                break
            errand = self.connector.fetch_and_format_errand(condi)
            if errand is None or errand.empty:
                continue
            df = self.single_connect(df, errand)
            
        self.df = df
        return self.df
    
    def do_connect(self) -> pd.DataFrame:
        """Categorize emails and connect them with errands."""
        pp =  PreProcess(self.services) 
        self.df = pp.do_preprocess(self.df)
        self.df = self.connect_with_time_windows()
        self.df = self.classifier.finalize_columns(self.df)
        return self.df

