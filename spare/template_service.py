"""
Template service - Handle forwarding templates
"""
from typing import Dict, Optional
import pandas as pd
from ..app.services.base_service import BaseService

class TemplateService(BaseService):
    """Template service"""
    
    def __init__(self):
        super().__init__()
        self._load_templates()
    
    def _load_templates(self):
        """Load template data"""
        try:
            self.forward_suggestions = pd.read_csv(f"{self.folder}/forwardSuggestion.csv")
            self.forward_format = pd.read_csv(f"{self.folder}/forwardFormat.csv")
        except Exception:
            self.forward_suggestions = pd.DataFrame()
            self.forward_format = pd.DataFrame()
    
    def get_subject_template(self, category: str) -> Optional[str]:
        """Get subject template"""
        try:
            template_row = self.forward_suggestions[
                (self.forward_suggestions['action'].str.startswith(category)) & 
                (self.forward_suggestions['action'].str.endswith('_Subject'))
            ]
            return template_row['templates'].iloc[0] if not template_row.empty else None
        except:
            return None
    
    def get_content_template(self, category: str) -> Optional[str]:
        """Get content template"""
        try:
            template_row = self.forward_suggestions[
                (self.forward_suggestions['action'].str.startswith(category)) & 
                (self.forward_suggestions['action'].str.endswith('_Template'))
            ]
            return template_row['templates'].iloc[0] if not template_row.empty else None
        except:
            return None
    
    def get_special_template(self, action: str) -> Optional[str]:
        """Get special template"""
        try:
            template_row = self.forward_suggestions[
                self.forward_suggestions['action'] == action
            ]
            return template_row['templates'].iloc[0] if not template_row.empty else None
        except:
            return None
    
    def render_template(self, template: str, context: Dict) -> str:
        """Render template"""
        if not template:
            return ""
        
        try:
            return template.format(**context)
        except KeyError as e:
            # Handle missing template variables
            return template.replace(f"{{{e.args[0]}}}", "")
        except Exception:
            return template
