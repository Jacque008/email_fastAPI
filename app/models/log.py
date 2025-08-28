"""
Log related business objects
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from datetime import datetime
from enum import Enum

class LogEventType(Enum):
    """Log event types"""
    ERRAND_CREATED = "Errand_Created"
    SEND_TO_IC = "Send_To_IC"
    UPDATE_DR = "Update_DR"
    EMAIL = "Email"
    CHAT = "Chat"
    COMMENT = "Comment"
    CREATE_INVOICE = "Create_Invoice"
    RECEIVE_PAYMENT_FROM_IC = "Receive_Payment_From_IC"
    RECEIVE_PAYMENT_FROM_DA = "Receive_Payment_From_DÄ"
    PAY_OUT_TO_CLINIC = "Pay_Out_To_CLinic"
    PAY_BACK_TO_CUSTOMER = "Pay_Back_To_Customer"
    ERRAND_CANCELLATION = "Errand_Cancellation"
    ERRAND_CANCELLATION_REVERSED = "Errand_Cancellation_Reversed"

@dataclass
class LogEvent:
    """Single log event"""
    timestamp: datetime
    event_type: LogEventType
    item_id: str
    message: str
    involved_party: str
    source: str = ""
    errand_id: int = 0
    
    def format_for_timeline(self) -> str:
        """Format for timeline display"""
        time_str = self.timestamp.strftime('%H:%M')
        
        if self.event_type == LogEventType.ERRAND_CREATED:
            return f"• At {time_str} (BOLD)Direktregleringsärendet skapades av klinik {self.involved_party}.(/BOLD)"
        elif self.event_type == LogEventType.SEND_TO_IC:
            return f"• At {time_str} (BOLD)Direktregleringsärendet skickades till försäkringsbolag {self.involved_party}.(/BOLD)"
        elif self.event_type == LogEventType.UPDATE_DR:
            return f"• At {time_str} (BOLD)Direktregleringsärendet uppdaterade ersättningsbeloppet {self.message} {self.involved_party}.(/BOLD)"
        elif self.event_type == LogEventType.EMAIL:
            source_text = "Klinik" if self.source == "Clinic" else ("Försäkringsbolag" if self.source == "Insurance_Company" else self.source)
            return f"• At {time_str} (BOLD){source_text} skickade ett {self.involved_party} emejl med följande innehåll:(/BOLD)"
        elif self.event_type == LogEventType.CHAT:
            return f"• At {time_str} (BOLD){self.involved_party} skickade ett chattmeddelande:(/BOLD)"
        elif self.event_type == LogEventType.COMMENT:
            return f"• At {time_str} (BOLD){self.involved_party} lämnade en kommentar:(/BOLD)"
        elif self.event_type == LogEventType.CREATE_INVOICE:
            return f"• At {time_str} (BOLD)En {self.involved_party} faktura skapades för {self.message}.(/BOLD)"
        elif self.event_type == LogEventType.RECEIVE_PAYMENT_FROM_IC:
            return f"• At {time_str} (BOLD)Mottog betalning på {self.message} från försäkringsbolag({self.involved_party}).(/BOLD)"
        elif self.event_type == LogEventType.RECEIVE_PAYMENT_FROM_DA:
            return f"• At {time_str} (BOLD)Mottog betalning på {self.message} från djurägare({self.involved_party}).(/BOLD)"
        elif self.event_type == LogEventType.PAY_OUT_TO_CLINIC:
            return f"• At {time_str} (BOLD)Betalade {self.message} till klinik {self.involved_party}.(/BOLD)"
        elif self.event_type == LogEventType.PAY_BACK_TO_CUSTOMER:
            return f"• At {time_str} (BOLD)Återbetalade {self.message} till djurägare {self.involved_party}.(/BOLD)"
        elif self.event_type == LogEventType.ERRAND_CANCELLATION:
            return f"• At {time_str} (BOLD)Direktregleringsärendet avslutades.(/BOLD)"
        elif self.event_type == LogEventType.ERRAND_CANCELLATION_REVERSED:
            return f"• At {time_str} (BOLD)Direktregleringsärendet återaktiverades.(/BOLD)"
        else:
            return f"• At {time_str} (BOLD){self.event_type.value}: {self.message}(/BOLD)"

@dataclass
class ErrandLog:
    """Complete errand log"""
    errand_id: int
    errand_number: str = ""
    events: List[LogEvent] = field(default_factory=list)
    ai_analysis: Optional[str] = None
    risk_score: Optional[float] = None
    payment_discrepancy: float = 0
    
    def add_event(self, event: LogEvent):
        """Add event to timeline"""
        self.events.append(event)
        self.events.sort(key=lambda x: x.timestamp)
    
    def calculate_payment_discrepancy(self, drp_fee: float = 149) -> float:
        """Calculate payment discrepancy"""
        discrepancy = 0
        for event in self.events:
            if event.event_type in [LogEventType.RECEIVE_PAYMENT_FROM_IC, LogEventType.RECEIVE_PAYMENT_FROM_DA]:
                try:
                    discrepancy += abs(float(event.source) if event.source else 0)
                except (ValueError, TypeError):
                    pass
            elif event.event_type in [LogEventType.PAY_OUT_TO_CLINIC, LogEventType.PAY_BACK_TO_CUSTOMER]:
                try:
                    discrepancy -= abs(float(event.source) if event.source else 0)
                except (ValueError, TypeError):
                    pass
        
        self.payment_discrepancy = discrepancy
        return discrepancy
    
    def generate_timeline_html(self) -> str:
        """Generate HTML timeline"""
        if not self.events:
            return ""
        
        discrepancy = self.calculate_payment_discrepancy()
        drp_fee = 149
        
        # Generate title
        if discrepancy == drp_fee or discrepancy == 0:
            title = f"Ärenden: {self.errand_id} °Betalningsavvikelse: Nej§"
        else:
            title = f"Ärenden: {self.errand_id} °Betalningsavvikelse: {int(discrepancy)}kr§"
        
        # Generate content
        content = []
        current_date = None
        placeholder = '€' * 11
        
        for event in self.events:
            event_date = event.timestamp.strftime('%Y-%m-%d')
            
            if current_date != event_date:
                if current_date is not None:
                    content.append("")  # Empty line to separate dates
                content.append(f"(COLORBLUE){event_date}(/SPAN)")
                current_date = event_date
            
            timeline_text = event.format_for_timeline()
            
            # Handle message content formatting
            if event.event_type in [LogEventType.EMAIL, LogEventType.CHAT, LogEventType.COMMENT]:
                formatted_msg = event.message.replace('\n', f'\n{placeholder}').rstrip('\n')
                content.append(f"{timeline_text}\n{placeholder}(ITALIC)(COLORGRAY){formatted_msg}(/SPAN)(/ITALIC)")
            else:
                content.append(timeline_text)
        
        # Merge content and convert to HTML
        full_text = title + "\n\n" + "\n".join(content)
        
        # HTML formatting
        html_content = (full_text
                       .replace('(COLORBLUE)', '<span style="color:blue;">')
                       .replace('(COLORGRAY)', '<span style="color:gray;">')
                       .replace('(ITALIC)', '<i>')
                       .replace('(/ITALIC)', '</i>')
                       .replace('(/SPAN)', '</span>')
                       .replace('(BOLD)', '<b>')
                       .replace('(/BOLD)', '</b>')
                       .replace('\n', '<br>')
                       .replace('€', '&nbsp;'))
        
        return html_content
    
    def get_title_and_content(self) -> tuple:
        """Get title and content"""
        html = self.generate_timeline_html()
        if '°' in html and '§' in html:
            title = html.split('°')[0]
            content = html.split('§')[1] if '§' in html else ""
            return title, content
        return f"Ärenden: {self.errand_id}", html
