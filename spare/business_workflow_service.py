"""
Business workflow service - Coordinate multiple business services to implement complete workflows
"""
from typing import List, Dict, Any
import pandas as pd
from ..app.models.forwarding import Forwardingemail
from ..app.models.log import ErrandLog, LogEvent, LogEventType
from ..app.models.text_content import CombinedText, TextContent, ContentType
from ..app.schemas.fw_email import ForwardingemailRequest, ForwardingemailResponse
from .log import ErrandLogRequest, ErrandLogResponse
from .text_content import CombinedTextRequest, CombinedTextResponse
from .template_service import TemplateService
from .address_service import AddressService  
from .text_processing_service import TextProcessingService
from .ai_service import AIService
from .data_service import DataService

class BusinessWorkflowService:
    """Business workflow service - Coordinate multiple business services to implement complete workflows"""
    
    def __init__(self):
        self.template_service = TemplateService()
        self.address_service = AddressService()
        self.text_service = TextProcessingService()
        self.ai_service = AIService()
        self.data_service = DataService()
    
    def process_email_categorization(self, emails_data: List[Dict]) -> List[Dict]:
        """Email categorization and connection workflow"""
        from ..app.dataset.email_dataset import EmailDataset
        from ..app.dataset.errand_dataset import ErrandDataset
        
        # Use existing dataset classes for processing
        email_dataset = EmailDataset(pd.DataFrame(emails_data))
        errand_dataset = ErrandDataset.from_db("er.\"createdAt\" >= NOW() - INTERVAL '45 day'")
        
        # Execute processing pipeline
        result = (email_dataset
                 .adjust_time()
                 .generate_content()
                 .detect_sender()
                 .detect_receiver()
                 .handle_vendor_specials()
                 .extract_attachments()
                 .extract_emails()
                 .initialize_categorize()
                 .enrich_staff_animal())
        
        # Connect errands
        final_result = result.finalize()
        
        return final_result.to_dict('records')
    
    def create_forward_email(self, request: ForwardingemailRequest) -> ForwardingemailResponse:
        """Create forward email workflow"""
        # 1. Get email data
        db_data = self.data_service.get_forward_email_data(request.id)
        if not db_data:
            raise ValueError(f"Email {request.id} not found")
        
        # 2. Create business object
        forward_email = Forwardingemail.from_request(request, db_data)
        
        # 3. Text processing
        processed_text = self.text_service.handle_colon_formatting(forward_email.original_content)
        processed_text = self.text_service.clean_email_beginning(processed_text)
        
        if forward_email.text_html:
            processed_text = self.text_service.extract_attachments_html(
                forward_email.text_html, processed_text
            )
        
        # 4. Get templates
        if forward_email.should_use_special_template():
            template = self.template_service.get_special_template('ProvetCloud_Template')
        else:
            template = self.template_service.get_content_template(forward_email.corrected_category)
        
        subject_template = self.template_service.get_subject_template(forward_email.corrected_category)
        
        # 5. Get admin information
        admin_name = ""
        if forward_email.user_id:
            admin_name = self.data_service.get_admin_name(forward_email.user_id)
        
        # 6. Generate summary information
        summary_info = self._generate_summary_info(forward_email, processed_text)
        
        # 7. Render templates
        context = {
            'WHO': forward_email.sender,
            'EMAIL': processed_text,
            'INFO': summary_info,
            'ADMIN': admin_name,
            'REFERENCE': self._get_reference_context(forward_email)
        }
        
        forward_email.forward_text = self.template_service.render_template(template or "", context)
        forward_email.forward_subject = self.template_service.render_template(
            subject_template or "", {'REFERENCE': forward_email.reference, 'WHO': forward_email.sender}
        )
        
        # 8. Format text
        format_rules = self.template_service.forward_format
        forward_email.forward_text = self.text_service.format_forward_text(
            forward_email.forward_text, format_rules
        )
        
        # 9. Determine forwarding address
        if forward_email.source == 'Clinic':
            forward_email.forward_address = self.address_service.(request.recipient)
        elif forward_email.source == 'Insurance_Company':
            forward_email.forward_address = self.address_service.get_clinic_address(request.recipient)
        
        return ForwardingemailResponse(
            id=forward_email.email_id,
            forward_address=forward_email.forward_address,
            forward_subject=forward_email.forward_subject,
            forward_text=forward_email.forward_text
        )
    
    def generate_errand_log(self, request: ErrandLogRequest) -> ErrandLogResponse:
        """Generate errand log workflow"""
        # 1. Get base data
        base_data = self.data_service.get_errand_base_data(request.errand_number)
        if not base_data:
            raise ValueError(f"Errand {request.errand_number} not found")
        
        errand_log = ErrandLog(
            errand_id=base_data['errandId'],
            errand_number=request.errand_number
        )
        
        # 2. Collect various event data
        events_data = {
            'create': self.data_service.get_errand_create_events(base_data),
            'send': self.data_service.get_send_to_ic_events(base_data),
            'email': self.data_service.get_email_events(request.errand_number),
            'chat': self.data_service.get_chat_events(request.errand_number),
            'comment': self.data_service.get_comment_events(request.errand_number),
            'update': self.data_service.get_update_events(base_data),
            'invoice': self.data_service.get_invoice_events(request.errand_number),
            'payment': self.data_service.get_payment_events(request.errand_number),
            'cancel': self.data_service.get_cancel_events(request.errand_number),
        }
        
        # 3. Convert to LogEvent objects and add to log
        for event_type, events in events_data.items():
            for event_data in events:
                log_event = self._convert_to_log_event(event_data, event_type)
                errand_log.add_event(log_event)
        
        # 4. Generate HTML timeline
        title, content = errand_log.get_title_and_content()
        
        # 5. Perform AI risk analysis
        timeline_text = errand_log.generate_timeline_html()
        ai_analysis = self.ai_service.perform_risk_assessment(timeline_text)
        
        return ErrandLogResponse(
            errand_id=errand_log.errand_id,
            timeline_html=content,
            ai_analysis=ai_analysis,
            risk_score=errand_log.risk_score
        )
    
    def generate_combined_text_summary(self, request: CombinedTextRequest) -> CombinedTextResponse:
        """Generate combined text summary workflow"""
        # 1. Build query conditions
        condition = self._build_text_condition(request)
        
        # 2. Get various data
        chat_data = self.data_service.get_chat_data(condition['chat'])
        email_data = self.data_service.get_email_data(condition['email'])
        comment_data = self.data_service.get_comment_data(condition['comment'])
        
        # 3. Create combined text object
        combined_text = CombinedText(
            errand_id=request.errand_id,
            email_id=request.email_id,
            reference=request.reference
        )
        
        # 4. Process and add content
        self._process_and_add_chat_content(combined_text, chat_data)
        self._process_and_add_email_content(combined_text, email_data)
        self._process_and_add_comment_content(combined_text, comment_data)
        
        # 5. Generate AI summary
        if combined_text.has_valid_content():
            formatted_input = combined_text.format_for_ai_analysis()
            summary = self.ai_service.generate_conversation_summary(formatted_input)
            combined_text.summary = summary
        else:
            combined_text.error_message = "Inga tillgängliga data"
        
        return CombinedTextResponse(
            summary=combined_text.summary or "",
            error_message=combined_text.error_message
        )
    
    def process_payment_matching(self, payments_data: List[Dict]) -> List[Dict]:
        """Payment matching workflow"""
        from .payment_service import PaymentService
        from .payment import PaymentMatchRequest
        
        payment_service = PaymentService()
        
        # Convert to request objects
        payment_requests = [
            PaymentMatchRequest(**payment_data) for payment_data in payments_data
        ]
        
        # Execute matching
        results = payment_service.match_payments(payment_requests)
        
        return [result.dict() for result in results]
    
    # Helper methods
    def _generate_summary_info(self, forward_email: Forwardingemail, text: str) -> str:
        """Generate summary information"""
        if '§' in text:
            return ''
        
        fields = {
            'Djurförsäkring: ': 'insuranceNumber',
            'SkadeNummer: ': 'damageNumber', 
            'Referens: ': 'reference',
            'Fakturanummer: ': 'invoiceReference',
            'Djurets namn: ': 'animalName',
            'Ägarens namn: ': 'ownerName'
        }
        
        # Add additional fields based on source
        if forward_email.source == 'Clinic':
            fields['Klinik: '] = 'sender'
            if forward_email.send_to == 'Insurance_Company':
                fields['Försäkringsbolag: '] = 'recipient'
        elif forward_email.source == 'Insurance_Company':
            fields['Försäkringsbolag: '] = 'sender'
            if forward_email.send_to == 'Clinic':
                fields['Klinik: '] = 'recipient'
        
        # Generate information string
        info_lines = []
        order = ['Klinik: ', 'Försäkringsbolag: ', 'Djurförsäkring: ', 'SkadeNummer: ', 
                'Referens: ', 'Fakturanummer: ', 'Djurets namn: ', 'Ägarens namn: ']
        
        for name in order:
            if name in fields:
                value = getattr(forward_email, fields[name], '')
                if value and pd.notna(value):
                    info_lines.append(f"{name}{value}§")
        
        if info_lines:
            return '<br><br><b>Ärendesammanfattning:</b><br>' + '\n'.join(info_lines)
        
        return ''
    
    def _get_reference_context(self, forward_email: Forwardingemail) -> str:
        """Get reference context"""
        if forward_email.corrected_category == 'Complement_DR_Insurance_Company':
            return f'&lt;mail+{forward_email.reference}@drp.se&gt;'
        elif forward_email.corrected_category == 'Question' and forward_email.reference:
            return f"eller skicka ett mail med kompletteringen till {forward_email.reference} "
        return forward_email.reference or ""
    
    def _build_text_condition(self, request: CombinedTextRequest) -> Dict[str, str]:
        """Build text query conditions"""
        if request.email_id:
            return {
                'chat': f"e.id = {request.email_id}",
                'email': f"e.id = {request.email_id}",
                'comment': f"e.id = {request.email_id}"
            }
        elif request.errand_id:
            return {
                'chat': f"e.errandId = {request.errand_id}",
                'email': f"e.errandId = {request.errand_id}",
                'comment': f"e.errandId = {request.errand_id}"
            }
        elif request.reference:
            return {
                'chat': f"ic.reference = '{request.reference}'",
                'email': f"ic.reference = '{request.reference}'", 
                'comment': f"ic.reference = '{request.reference}'"
            }
        else:
            return {'chat': '', 'email': '', 'comment': ''}
    
    def _convert_to_log_event(self, event_data: Dict, event_type: str) -> LogEvent:
        """Convert data to LogEvent object"""
        # Convert based on event type
        event_type_mapping = {
            'create': LogEventType.ERRAND_CREATED,
            'send': LogEventType.SEND_TO_IC,
            'email': LogEventType.EMAIL,
            'chat': LogEventType.CHAT,
            'comment': LogEventType.COMMENT,
            'update': LogEventType.UPDATE_DR,
            'invoice': LogEventType.CREATE_INVOICE,
            'payment': self._determine_payment_type(event_data),
            'cancel': LogEventType.ERRAND_CANCELLATION,
        }
        
        return LogEvent(
            timestamp=event_data['timestamp'],
            event_type=event_type_mapping.get(event_type, LogEventType.COMMENT),
            item_id=event_data.get('itemId', ''),
            message=event_data.get('message', ''),
            involved_party=event_data.get('involved', ''),
            source=event_data.get('source', ''),
            errand_id=event_data.get('errandId', 0)
        )
    
    def _determine_payment_type(self, event_data: Dict) -> LogEventType:
        """Determine payment event type"""
        node = event_data.get('node', '')
        if 'IC' in node:
            return LogEventType.RECEIVE_PAYMENT_FROM_IC
        elif 'DÄ' in node:
            return LogEventType.RECEIVE_PAYMENT_FROM_DA
        elif 'Clinic' in node:
            return LogEventType.PAY_OUT_TO_CLINIC
        elif 'Customer' in node:
            return LogEventType.PAY_BACK_TO_CUSTOMER
        else:
            return LogEventType.RECEIVE_PAYMENT_FROM_IC
    
    def _process_and_add_chat_content(self, combined_text: CombinedText, chat_data: List[Dict]):
        """Process and add chat content"""
        for chat in chat_data:
            if chat.get('message'):
                content = TextContent(
                    content_id=chat['id'],
                    content_type=ContentType.CHAT,
                    timestamp=pd.to_datetime(chat['createdAt']),
                    source=self._determine_chat_source(chat),
                    content=chat['message']
                )
                combined_text.add_content(content)
    
    def _process_and_add_email_content(self, combined_text: CombinedText, email_data: List[Dict]):
        """Process and add email content"""
        for email in email_data:
            if email.get('email'):
                content = TextContent(
                    content_id=email['id'],
                    content_type=ContentType.EMAIL,
                    timestamp=pd.to_datetime(email['createdAt']),
                    source=email.get('source', 'Unknown'),
                    content=email['email']
                )
                combined_text.add_content(content)
    
    def _process_and_add_comment_content(self, combined_text: CombinedText, comment_data: List[Dict]):
        """Process and add comment content"""
        for comment in comment_data:
            if comment.get('content'):
                content = TextContent(
                    content_id=comment['id'],
                    content_type=ContentType.COMMENT,
                    timestamp=pd.to_datetime(comment['createdAt']),
                    source='DRP',
                    content=comment['content']
                )
                combined_text.add_content(content)
    
    def _determine_chat_source(self, chat_data: Dict) -> str:
        """Determine chat source"""
        if chat_data.get('fromClinicUserId'):
            return 'Clinic'
        elif chat_data.get('fromInsuranceCompanyId'):
            return 'Insurance_Company'
        elif chat_data.get('fromAdminUserId'):
            return 'DRP'
        else:
            return 'Unknown'
