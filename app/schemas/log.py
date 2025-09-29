from typing import Dict
from pydantic import BaseModel, Field, RootModel

class LogIn(BaseModel):
    """Input schema for chronological log requests"""
    errandNumber: str = Field(description="Errand number to generate log for")

class LogOut(RootModel[Dict[str, Dict[str, str]]]):
    """Output schema for chronological log results - nested structure like:
    {
        "Ärenden: 69025": {
            "AI_Analysis": "...",
            "Chronological_Log": "<br>..."
        }
    }
    """
    root: Dict[str, Dict[str, str]]

