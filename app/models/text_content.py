"""
Combined text content business objects
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum

class ContentType(Enum):
    """Content types"""
    EMAIL = "email"
    CHAT = "chat"
    COMMENT = "comment"

@dataclass
class TextContent:
    """Single text content item"""
    content_id: int
    content_type: ContentType
    timestamp: datetime
    source: str  # Clinic, Insurance_Company, DRP
    content: str
    metadata: Dict = field(default_factory=dict)

@dataclass
class CombinedText:
    """Combined text content object"""
    errand_id: Optional[int] = None
    email_id: Optional[int] = None
    reference: Optional[str] = None
    contents: List[TextContent] = field(default_factory=list)
    summary: Optional[str] = None
    error_message: Optional[str] = None
    
    def add_content(self, content: TextContent):
        """Add text content"""
        self.contents.append(content)
        self.contents.sort(key=lambda x: x.timestamp)
    
    def get_by_type(self, content_type: ContentType) -> List[TextContent]:
        """Get content by type"""
        return [c for c in self.contents if c.content_type == content_type]
    
    def get_by_source(self, source: str) -> List[TextContent]:
        """Get content by source"""
        return [c for c in self.contents if c.source == source]
    
    def format_for_ai_analysis(self) -> str:
        """Format for AI analysis input"""
        if not self.contents:
            return ""
        
        formatted_lines = []
        for content in self.contents:
            role = f"{content.content_type.value} from {content.source}"
            formatted_lines.append(f"{role}: {content.content.strip()}")
        
        return "\n".join(formatted_lines)
    
    def has_valid_content(self) -> bool:
        """Check if has valid content"""
        return any(content.content.strip() for content in self.contents)
