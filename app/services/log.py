import pandas as pd
import re
from typing import Dict, Tuple, Optional, Any, List
from .base_service import BaseService
from .processor import Processor
from .utils import (fetchFromDB,
                    skip_thinking_part,
                    get_groq_client,
                    tz_convert,
                    groq_chat_with_fallback,
                    parse_payex_xml)


class LogService(BaseService):
    """
    Service for generating chronological logs of errands with risk assessment.
    Refactored from chronologicalLog.py with improved efficiency.
    """
    
    def __init__(self):
        super().__init__()
        self.cond = True 
        self.processor = Processor()  # Initialize processor for text merging
        self._setup_mappings()
        self._setup_rules()
        self._setup_system_prompt()

    def setup_query_conditions(self, errand_number: str):
        """Setup query conditions and reload queries with new parameters"""
        self.cond = f"er.\"reference\" = '{errand_number}'"

        self.log_base_query = (self.queries['logBase'].iloc[0]).format(COND=self.cond)
        self.log_email_query = (self.queries['logEmail'].iloc[0]).format(COND=self.cond)
        self.log_chat_query = (self.queries['logChat'].iloc[0]).format(COND=self.cond)
        self.log_comment_query = (self.queries['logComment'].iloc[0]).format(COND=self.cond)
        self.log_original_invoice_query = (self.queries['logOriginalInvoice'].iloc[0]).format(COND=self.cond)
        self.log_vet_fee_query = (self.queries['logVetFee'].iloc[0]).format(COND=self.cond)
        self.log_payment_option_query = (self.queries['logPaymentOption'].iloc[0]).format(COND=self.cond)
        self.log_invoice_sp_query = (self.queries['logInvoiceSP'].iloc[0]).format(COND=self.cond)
        self.log_invoice_fortus_query = (self.queries['logInvoiceFortus'].iloc[0]).format(COND=self.cond)
        self.log_invoice_ka_query = (self.queries['logInvoiceKA'].iloc[0]).format(COND=self.cond)
        self.log_invoice_fortnox_query = (self.queries['logInvoiceFortnox'].iloc[0]).format(COND=self.cond)
        self.log_invoice_px_query = (self.queries['logInvoicePayex'].iloc[0]).format(COND=self.cond)
        self.log_receive_query = (self.queries['logReceive'].iloc[0])
        self.log_cancel_query = (self.queries['logCancel'].iloc[0]).format(COND=self.cond)
        self.log_remove_cancel_query = (self.queries['logRemoveCancel'].iloc[0]).format(COND=self.cond)
        self.groq_client = get_groq_client()
        self.model = self.model_df['model'].iloc[0] if not self.model_df.empty else "deepseek-r1-distill-llama-70b"
    
    def _setup_system_prompt(self):
        self.system_prompt = """
            You are an expert in log analysis and risk assessment. 
            Your task is to analyze errand logs in Swedish based on the following predefined steps and logical rules:

            ### Predefined Steps:
            {BASIC_STEPS}

            ### Time Rules:
            {TIME_RULES}

            ### Payment Rules:
            {PAYMENT_RULES}

            ### Logical Rules:
            {LOGICAL_RULES}

            ### Analysis Guidelines
            For each errand log, you should:

            1. Verify Step Completion:
            - If the errand has a "Payout to Clinic" step, check if all required steps are present.
            - If the errand does not have a "Payout to Clinic" step, list only the next steps that need to be performed.
            - Missing optional steps (marked as optional in predefined steps) should not trigger an alert and should not be listed in the response.
            - If an errand is canceled, do not list missing steps or next steps. Instead, analyze the cancellation reason.

            2. Identify Time-Related Issues:
            - Detect incorrect time sequences based on the time rules.
            - Only flag unexpected time discrepancies, such as:
                - A payment occurring before invoicing.
                - A long gap between steps that may indicate a delay (e.g., more than 30 days between compensation update and insurance payment).
            - Do not mention expected time sequences unless there is an issue.

            3. Check Payment Consistency:
            - If there is 'Betalningsavvikelse: xxx kr' in the first line of the log, only flag the discrepancy and provide the exact difference.
            - If the first line states 'Betalningsavvikelse: Nej', respond with 'Inga'.
            - Invoice amounts are only relevant to the customer and do not need to match the amount paid to the clinic.
            - Ensure that:
                - Payment from the insurance company matches the updated compensation amount.
                - Payment from the customer matches the invoice amount.

            4. Detect Exceptions:
            - Identify anomalies such as:
                - Duplicate payments.
                - Mismatch between payment from the insurance company and the updated compensation amount.
                - Mismatch between payment from the customer and the invoice amount.
                - Unexpected deviations from the predefined steps or logical rules.
            - Do not mention actions that follow the logical rules (e.g., "Compensation amount updated directly in an email" should not be listed if allowed).

            5. Assess Risk Level:
            - Assign a risk level (Low, Medium, or High) based on detected issues.
            - If the risk level is low, do not provide any reason.
            - Provide a brief justification with format bullet list only if the risk level is Medium or High.
            - If the errand is canceled:
                - If canceled by the clinic, consider it Low risk.
                - Otherwise, assess the risk based on the cancellation reason.

            6. Special Risk Warnings:
            - If 30 days have passed since the compensation amount was updated without payment from the insurance company, flag this as a risk.
            - If 30 days have passed since the invoice was created without payment from the customer, flag this as a risk.

            ### Output Format Template
            COMPLETION STATUS: {COMPLETE}
            
            CRITICAL INSTRUCTION: Since completion status is {COMPLETE}, follow these rules:
            - If {COMPLETE} = "True": Write "Ja" for Ärenden avslutade and DO NOT include Saknade steg or Nästa steg lines
            - If {COMPLETE} = "False": Write "Nej" for Ärenden avslutade and include Saknade steg and Nästa steg lines
            
            EXACT OUTPUT FORMAT (copy this template):

            ******************* Sammanfattning och Analys with model §model_name§ *********************
            - Ärenden avslutade:  {ARENDEN_STATUS}
            {CONDITIONAL_SECTIONS}
            - Tidsproblem:  [List time problems or write " Inga"]
            - Betalningsavvikelser:  [List payment discrepancies or write " Inga"]
            - Undantag:  [List exceptions or write " Inga"]
            - Särskilda riskvarningar:  [List special warnings or write " Inga"]
            - Anledning till avbokning:  [List cancellation reason or write " Inga"]
            - Risknivå:  [Write " Låg", " Medel" or " Hög"]
                • [Motivation 1 - only if risk is Medel or Hög]
                • [Motivation 2 - only if risk is Medel or Hög]
                        """
        
    def _setup_mappings(self):
        """Setup column mappings and category mappings"""
        self.columns = ['errandId', 'node', 'timestamp', 'itemId', 'msg', 'involved', 'source']
        self.col_mapping = {'node': 'Nod', 'timestamp': 'Tid', 'msg': 'Innehåll', 'involved': 'Inblandade'}
        self.category_mapping = {
            'Auto_Reply': 'Auto-Svar',
            'Finance_Report': 'Ekonomirapport', 
            'Information': 'Generell Information',
            'Settlement_Request': 'Förhandsbesked',
            'Message': 'Specifik Information',
            'Question': 'Fråga',
            'Settlement_Approved': 'Ersättningsbesked',
            'Settlement_Denied': 'Ersättning Nekad',
            'Wisentic_Error': 'Fel i Djurskador',
            'Complement_Damage_Request_Insurance_Company': 'Komplettering Skadeanmälan',
            'Other': 'Övrigt',
            'Spam': 'Spam',
            'Complement_Damage_Request_Clinic': 'Komplettering Skadeanmälan',
            'Complement_DR_Insurance_Company': 'Komplettering Direktreglering',
            'Complement_DR_Clinic': 'Komplettering Direktreglering',
            'Insurance_Validation_Error': 'Felaktig Försäkringsinfo',
            'Manual_Handling_Required': 'Manuell Hantering'}
        self.lappland_ids = ['28', '29', '50', '51', '52', '53', '54', '55', '56', '79', '81', '82', '83', '84', '85'] 
        self.lappland_names = ['Djurkliniken i Skellefteå','Skellefteå Anderstorg','Din Veterinär Piteå','Lapplands Djurklinik Jokkmokk',
                               'Lapplands Djurklinik Arvidsjaur','Lapplands Djurklinik Boden','Lapplands djurklinik Luleå',
                               'Lapplands Djurklinik Kiruna','Lapplands Djurklinik Gällivare', 'våra vänner Umeå','våra vänner Bromma',
                               'våra vänner Barkarby','våra vänner Karlaplan','våra vänner Luleå','våra vänner Göteborg']
 
    def _setup_rules(self):
        """Setup business rules for risk assessment"""
        self.basic_steps = {
            "Errand_Created":"Errand Created (Required and Errand Started)",
            "Send_To_IC": "Errand Submitted to Insurance Company (Required)",
            "Email": "Email Correspondence Between Insurance Company and Clinic (Optional)",
            "Update_DR": "Compensation Amount Updated (Required)",
            "Chat": "Chat Communication Between Insurance Company, Clinic, and DRP (Optional)",
            "Comment": "Comment Added (Optional)",
            "Vet_Fee": "Vet fee needs to pay (Optional)",
            "Create_Invoice": "Invoice Generated (Required)",
            "Receive_Payment_From_FB": "Payment Received from Insurance Company (Required)",
            "Receive_Payment_From_DÄ": "Payment Received from animal owner (Required)",
            "Pay_Back_To_Customer": "Payment to animal owner (Optional)",
            "Errand_Cancellation": "Errand was cancelled (Optional)",
            "Errand_Cancellation_Reversed": "Errand was cancelled before but reinstated later (Optional)",
            "Pay_Out_To_CLinic": "Payment to Clinic (Required and Errand Closed)"
        }

        self.required_steps = {
            "Errand_Created": "Skapa ärende",
            "Send_To_IC": "Skicka ärendet till försäkringsbolag",
            "Update_DR": "Uppdatera ersättningsbeloppet",
            "Create_Invoice": "Skapa faktura för kunden",
            "Receive_Payment_From_FB": "Motta betalning från försäkringsbolag",
            "Receive_Payment_From_DÄ": "Motta betalning från kund",
            "Pay_Out_To_CLinic": "Göra utbetalning till klinik"
        }
        
        self.time_rules = [
            "Errand submission must occur after creation.",
            "Invoicing must occur after compensation amount updates.",
            "It is normal for email or chat conversations between the insurance company and the clinic to occur over multiple days.",
            "Insurance company payments must occur after compensation amount updates, a delay of several days is normal.",
            "Customer payments must occur after invoicing, a delay of several days is normal.",
            "Errand can be cancelled at anytime."
        ]
        
        self.payment_rules = [
            "Compensation amount updates can be performed via the Agria API, auto-matching, directly in email, or manually.",
            "Payment from the insurance company must match the updated compensation amount.",
            "Payment from the customer must match the invoice amount.",
            "Invoices are issued only to customers, not to the insurance company.",
            "Compensation does not need to be related to the invoice amount."
        ]
        
        self.logical_rules = [
            "If an action is normal or follows the logical rules, do not mention it in the response.",
            "If any action violates the logical rules, explicitly mention it in the response."
        ]
    
    def get_errand_base_data(self) -> pd.DataFrame:
        """Get base errand data with optimized data types"""
        base = fetchFromDB(self.log_base_query)
        if base.empty:
            return pd.DataFrame()

        float_cols = base.select_dtypes(include=['float64']).columns
        base[float_cols] = base[float_cols].astype('Int64')

        return base
    
    def create_errand_log(self, base: pd.DataFrame) -> pd.DataFrame:
        """Create errand creation log entries"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        create = base[['errandId', 'errandCreaTime', 'errandNumber', 'clinicName']].copy()
        if create.empty:
            return pd.DataFrame(columns=self.columns)
        
        create['node'] = 'Errand_Created'
        create['msg'] = ''
        create['source'] = ''
        create = create.rename(columns={
            "errandCreaTime": "timestamp",
            "errandNumber": "itemId",
            "clinicName": "involved"
        }).drop_duplicates()
        create['itemId'] = 'errandNr: ' + create['itemId'].astype(str)
        create = tz_convert(create, 'timestamp')

        return create[self.columns]
    
    def send_to_ic_log(self, base: pd.DataFrame) -> pd.DataFrame:
        """Create send to insurance company log entries"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        send = base.loc[base['sendTime'].notna(), [
            'errandId', 'insuranceCaseId', 'reference', 'sendTime', 'insuranceCompanyName'
        ]].copy()
        
        if send.empty:
            return pd.DataFrame(columns=self.columns)
        
        send['node'] = 'Send_To_IC'
        send['source'] = ''
        send = send.rename(columns={
            "insuranceCaseId": "itemId",
            "sendTime": "timestamp", 
            "reference": "msg",
            "insuranceCompanyName": "involved"
        }).drop_duplicates()

        send = tz_convert(send, 'timestamp')
        send['itemId'] = 'insuranceCaseId: ' + send['itemId'].astype(str)
        send['msg'] = 'reference: ' + send['msg'].astype(str)

        return send[self.columns]
    
    def get_email_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Get and process email data with efficiency optimizations"""
        email_base = fetchFromDB(self.log_email_query)
        email_base_columns = ['errandId', 'emailId', 'subject', 'textPlain', 'textHtml', 
                             'emailTime', 'category', 'correctedCategory', 'source']
        if email_base.empty:
            return pd.DataFrame(columns=self.columns), pd.DataFrame(columns=email_base_columns)
        
        email_base = email_base[email_base_columns]
        email_base.loc[:,['origin', 'email']] = email_base.apply(
            lambda row: self.processor.merge_html_text(row['subject'], row['textPlain'], row['textHtml']), 
            axis=1
        ).apply(pd.Series)
        
        email = email_base.copy()
        email['node'] = 'Email'
        email['correctedCategory'] = email['correctedCategory'].fillna(email['category'])
        email['correctedCategory'] = email['correctedCategory'].map(self.category_mapping)
        email = email.rename(columns={
            "emailId": "itemId",
            "emailTime": "timestamp",
            "email": "msg", 
            "correctedCategory": "involved"
        }).drop_duplicates()
        email['itemId'] = 'emailId: ' + email['itemId'].astype(str)
        email = tz_convert(email, 'timestamp')

        return email[self.columns], email_base
    
    def get_chat_data(self, base: pd.DataFrame) -> pd.DataFrame:
        """Get and process chat data"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        chat = fetchFromDB(self.log_chat_query)
        if chat.empty:
            return pd.DataFrame(columns=self.columns)
        
        chat = chat.merge(base, on='errandId', how='inner')
        chat['node'] = 'Chat'
        chat['involved'] = ''
        chat['source'] = ''
              
        chat.loc[chat['chatDRP'].notna(),   'involved'] = chat.loc[chat['chatDRP'].notna(),   'chatDRP']
        chat.loc[chat['chatClinic'].notna(),'involved'] = chat.loc[chat['chatClinic'].notna(),'chatClinic']
        chat.loc[chat['chatFB'].notna(),    'involved'] = chat.loc[chat['chatFB'].notna(),    'chatFB']
        
        chat = chat.rename(columns={
            "chatMessageId": "itemId",
            "chatTime": "timestamp",
            "message": "msg"
        }).drop_duplicates()
        
        chat['itemId'] = 'chatMessageId: ' + chat['itemId'].astype(str)
        chat = tz_convert(chat, 'timestamp')

        return chat[self.columns]
    
    def get_comment_data(self, base: pd.DataFrame) -> pd.DataFrame:
        """Get and process comment data"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        comment = fetchFromDB(self.log_comment_query)
        if comment.empty:
            return pd.DataFrame(columns=self.columns)
        
        comment = comment.merge(base, on='errandId', how='inner')
        comment = comment[['errandId', 'commentId', 'commentTime', 'commentDRP', 'content']]
        comment['node'] = 'Comment'
        comment['source'] = ''
        
        comment = comment.rename(columns={
            "commentId": "itemId",
            "commentTime": "timestamp",
            "commentDRP": "involved",
            "content": "msg"
        }).drop_duplicates()
        
        comment['itemId'] = 'commentId: ' + comment['itemId'].astype(str)
        comment = tz_convert(comment, 'timestamp')

        return comment[self.columns]
    
    def create_update_errand_log(self, base: pd.DataFrame, email_base: pd.DataFrame) -> pd.DataFrame:
        """Create errand update log entries with optimized logic"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        update = base.loc[(base['settlementAmount'].notna()) & (base['updatedTime'].notna()), [
            'errandId', 'insuranceCaseId', 'updatedTime', 'settlementAmount'
        ]].copy()
        
        if update.empty:
            return pd.DataFrame(columns=self.columns)

        involved = update.merge(email_base[['errandId', 'category', 'correctedCategory']], 
                           on='errandId', how='left')
        involved['involved'] = None
        involved.loc[involved['category'].isna(), 'involved'] = 'by Agria API'
        involved.loc[
            involved['category'].str.startswith('Settlement', na=False) & involved['correctedCategory'].isna(), 
            'involved'] = 'by auto-matching'
        involved.loc[
            involved['correctedCategory'].notna() & involved['correctedCategory'].str.startswith('Settlement', na=False), 
            'involved'] = 'manually'
        
        if involved['involved'].isna().all():
            involved.loc[
                involved['settlementAmount'].notna() & involved['involved'].isna(), 
                'involved'] = 'directly in email'
        else:
            involved = involved.dropna(subset=['involved'])
        
        update['node'] = 'Update_DR'
        update['source'] = ''
        update = update.merge(involved[['errandId', 'involved']], on='errandId', how='left')
        
        update = update.rename(columns={
            "insuranceCaseId": "itemId",
            "updatedTime": "timestamp",
            "settlementAmount": "msg"
        }).drop_duplicates()
        
        update['msg'] = update['msg'].astype(str) + ' kr'
        update['itemId'] = 'insuranceCaseId: ' + update['itemId'].astype(str)
        update = tz_convert(update, 'timestamp')

        return update[self.columns]
     
    def get_vet_fee_data(self) -> pd.DataFrame:
        try:
            vetfee = fetchFromDB(self.log_vet_fee_query)
            if vetfee.empty:
                return pd.DataFrame(columns=self.columns)
        except Exception as e:
            raise Exception(f"failed fetch data from Database: - {str(e)}")

        vetfee['node'] = 'Vet_Fee'
        vetfee['source'] = int(vetfee['vetFeeAmount'].iloc[0])
        vetfee['name'] = vetfee['name'].fillna('').astype(str).str.replace(' levreskontra', '', regex=False).str.strip()
        vetfee = vetfee.rename(columns={
            "reference": "itemId",
            "transTime": "timestamp",
            "vetFeeAmount": "msg",
            "name": "involved"
        }).drop_duplicates()
        
        vetfee['itemId'] = vetfee['itemId'].apply(lambda x: f'reference: {x}' if pd.notna(x) else "No vet fee")
        vetfee['msg'] = vetfee['msg'].apply(lambda x: f"{str(x).replace('-', '')} kr" if pd.notna(x) else "No vet fee")
        vetfee = tz_convert(vetfee, 'timestamp')

        return vetfee[self.columns]
        
    def get_invoice_data(self) -> pd.DataFrame:
        """Get and process invoice data from multiple sources"""
        invoice_queries_map = {
            "swedbank": self.log_invoice_sp_query,
            "fortus": self.log_invoice_fortus_query,
            "kassa": self.log_invoice_ka_query, 
            "datacentralen": self.log_invoice_fortnox_query,
            "datacentralen12": self.log_invoice_fortnox_query,
            "payex": self.log_invoice_px_query,
        }
        invoice_dfs = []
        payment_option_df = fetchFromDB(self.log_payment_option_query)
        if not payment_option_df.empty:
            payment_option = payment_option_df['paymentOption'].iloc[0]
            if payment_option and payment_option in invoice_queries_map:
                query = invoice_queries_map[payment_option]

                try:
                    df = fetchFromDB(query)
                    if not df.empty:
                        if payment_option == 'payex':
                            try:
                                file_name = df['fileName'].iloc[0]
                                customer_id = df['animalOwnerId'].iloc[0]
                                file_path = f"payex/incoming/{file_name}"
                                payex_invoice_df = parse_payex_xml(file_path, customer_id)
                                df = df.merge(payex_invoice_df, on=['animalOwnerId', 'invoiceNumber'], how='left')

                            except Exception as payex_error:
                                raise Exception(f"Warning: Payex XML parsing failed: {str(payex_error)}")

                        invoice_dfs.append(df)
                except Exception as e:
                    raise Exception(f"failed for fetch data from database: - {str(e)}")
        
        if not invoice_dfs:
            return pd.DataFrame(columns=self.columns)
        
        invoice = pd.concat(invoice_dfs, ignore_index=True)
        invoice['node'] = 'Create_Invoice'
        invoice['source'] = ''
        
        invoice = invoice.rename(columns={
            "invoiceNumber": "itemId",
            "transTime": "timestamp",
            "invoiceAmount": "msg",
            "paymentOption": "involved"
        }).drop_duplicates()
        
        invoice['itemId'] = invoice['itemId'].apply(lambda x: f'invoiceNr: {x}' if pd.notna(x) else "No Invoice")
        invoice['msg'] = invoice['msg'].apply(lambda x: f"{str(x).replace('-', '')} kr" if pd.notna(x) else "No Invoice")
        invoice = tz_convert(invoice, 'timestamp')

        return invoice[self.columns]
    
    def get_payment_data(self) -> pd.DataFrame:
        """Get and process payment data from multiple sources with optimized queries"""
        # Define payment conditions
        payment_conditions = [
            (" AND a.\"ownerType\"='insurance_company' AND a.type_='receivable' AND tl.type_='settlement_payment_line' AND es.\"settlementPaid\" IS TRUE", 'Receive_Payment_From_FB'),
            (" AND a.\"ownerType\"='animal_owner' AND a.type_='receivable' AND tl.type_='customer_payment_line' AND es.\"customerPaid\" IS TRUE", 'Receive_Payment_From_DÄ'),
            (" AND a.\"ownerType\"='clinic' AND a.type_='cash' AND tl.type_='veterinary_payout_line' AND es.\"disbursed\" IS TRUE", 'Pay_Out_To_CLinic'),
            (" AND a.\"ownerType\"='animal_owner' AND a.type_='receivable' AND tl.type_='customer_reversal_line'", 'Pay_Back_To_Customer')
        ]
        
        payment_dfs = []
        for cond_suffix, node_type in payment_conditions:
            try:
                full_cond = str(self.cond) + cond_suffix
                df = fetchFromDB(self.log_receive_query.format(COND=full_cond))
                if not df.empty:
                    df['node'] = node_type
                    mask = df['accountingDate'].isna()
                    df.loc[mask, 'accountingDate'] = df.loc[mask, 'createdAt']
                    payment_dfs.append(df)
            except Exception:
                continue
        
        if not payment_dfs:
            return pd.DataFrame(columns=self.columns)
        
        payment = pd.concat(payment_dfs, ignore_index=True)
        payment['source'] = payment['amount']
        
        payment = payment.rename(columns={
            "transactionId": "itemId",
            "accountingDate": "timestamp",
            "name": "involved",
            "amount": "msg"
        }).drop_duplicates()
        
        payment['msg'] = payment['msg'].apply(lambda x: f"{str(x).replace('-', '')} kr")
        payment['itemId'] = 'transactionId: ' + payment['itemId'].astype(str)
        payment['involved'] = payment['involved'].str.replace(r'( klientmedel| kundreskontra)$', '', regex=True)
        payment = tz_convert(payment, 'timestamp')

        return payment[self.columns]
    
    def get_cancellation_data(self) -> pd.DataFrame:
        """Get cancellation data"""
        cancel = fetchFromDB(self.log_cancel_query)
        if cancel.empty:
            return pd.DataFrame(columns=self.columns)
        
        cancel['node'] = 'Errand_Cancellation'
        cancel['msg'] = 'Cancelled'
        cancel['involved'] = ''
        cancel['source'] = ''
        
        cancel = cancel.rename(columns={
            "transactionId": "itemId",
            "cancelTime": "timestamp"
        }).drop_duplicates()
        
        cancel['itemId'] = 'transactionId: ' + cancel['itemId'].astype(str)
        cancel = tz_convert(cancel, 'timestamp')

        return cancel[self.columns]
    
    def get_reversal_data(self) -> pd.DataFrame:
        """Get cancellation reversal data"""
        remove = fetchFromDB(self.log_remove_cancel_query)
        if remove.empty:
            return pd.DataFrame(columns=self.columns)
        
        remove['node'] = 'Errand_Cancellation_Reversed'
        remove['msg'] = 'Reinstated'
        remove['involved'] = ''
        remove['source'] = ''
        
        remove = remove.rename(columns={
            "transactionId": "itemId",
            "removeTime": "timestamp"
        }).drop_duplicates()
        
        remove['itemId'] = 'transactionId: ' + remove['itemId'].astype(str)
        remove = tz_convert(remove, 'timestamp')

        return remove[self.columns]
    
    def create_formatted_log(self, base: pd.DataFrame, *log_dfs) -> Tuple[Dict, Dict]:
        """Create formatted chronological log with optimized processing"""
        def filter_columns(df):
            return df.loc[:, df.notna().any()] if not df.empty else df

        processed_dfs = [filter_columns(df) for df in log_dfs if not df.empty]
        
        if not processed_dfs:
            return {}, {}
        
        log = pd.concat(processed_dfs, ignore_index=True).drop_duplicates()
        log = log.merge(base[['errandId', 'clinicId', 'clinicName', 'insuranceCompanyName', 'complete']], on='errandId')
        
        float_cols = log.select_dtypes(include=['float64']).columns
        log[float_cols] = log[float_cols].astype('Int64')
        
        log = log[['errandId', 'node', 'timestamp', 'itemId', 'msg', 'involved', 
                  'source', 'clinicId', 'clinicName', 'insuranceCompanyName', 'complete']].sort_values('timestamp')
        
        grouped = log.groupby('errandId')
        group_log, group_ai = {}, {}
        
        for group_id, group_df in grouped:
            group_df = group_df.sort_values('timestamp').reset_index(drop=True).copy()
            complete_nodes = group_df['node'].drop_duplicates().to_list()

            clinic_id = group_df['clinicId'].iloc[0]
            drp_fee_query = f'SELECT (c."apoexFeeAmount" / 100) AS "drp_fee"  FROM clinic c WHERE c.id = {clinic_id}'
            clinic_drp_fee_df = fetchFromDB(drp_fee_query)
            drp_fee = int(clinic_drp_fee_df['drp_fee'].iloc[0]) if not clinic_drp_fee_df.empty else 199

            paragraph, discrepancy = self._generate_chronologic_log(group_id, group_df, drp_fee)
            if discrepancy == 0:
                title = f"Ärenden: {group_id} °Betalningsavvikelse: Nej§"
            else:
                title = f"Ärenden: {group_id} °Betalningsavvikelse: {int(discrepancy)}kr§"
            
            paragraph = title + "\n\n" + paragraph[len(f"Ärenden: {group_id}\n\n"):]
            
            formatted_log = self._format_for_html(paragraph)

            group_log[group_id] = {
                "title": formatted_log.split('°', 1)[0],
                "content": formatted_log.split('§', 1)[1]
            }
            errand_complete = group_df['complete'].iloc[0]

            clean_text = self._clean_before_feed_in_model(paragraph)
            ai_analysis = self.generate_risk_assessment(clean_text, errand_complete, complete_nodes)
            group_ai[group_id] = self._format_ai_analysis(ai_analysis)
        
        return group_log, group_ai
    
    def _generate_chronologic_log(self, group_id: Any, group_df: pd.DataFrame, drp_fee: int) -> Tuple[str, float]:
        """Generate log content for a specific errand group"""
        paragraph = f"Ärenden: {group_id}\n\n"
        origin_invoice = fetchFromDB(self.log_original_invoice_query)
        discrepancy = origin_invoice['invoiceAmount'].sum() + drp_fee
        date_cache = {}
        placeholder = '€' * 11
        
        for idx, (_, row) in enumerate(group_df.iterrows()):
            
            timestamp_str = str(row['timestamp'])
            time_part = timestamp_str[11:16] if len(timestamp_str) >= 16 else timestamp_str[11:]
            date_cache[idx] = timestamp_str[:10]
            if idx > 0 and (idx - 1) in date_cache and date_cache[idx] == date_cache[idx - 1]:
                show_date = f'• At {time_part}'
            else:
                show_date = '(COLORBLUE)' + date_cache[idx] + '(/SPAN)\n• At ' + time_part
            
            if row['node'] == 'Errand_Created':
                paragraph += f"{show_date} (BOLD)Direktregleringsärendet skapades av klinik (COLORRED){row['involved']}(/SPAN).(/BOLD)\n"
            
            elif row['node'] == 'Send_To_IC':
                paragraph += f"{show_date} (BOLD)Direktregleringsärendet skickades till försäkringsbolag (COLORRED){row['involved']}(/SPAN).(/BOLD)\n"
            
            elif row['node'] == 'Update_DR':
                paragraph += f"{show_date} (BOLD)Direktregleringsärendet uppdaterade ersättningsbeloppet (COLORRED){row['msg']}(/SPAN) {row['involved']}.(/BOLD)\n"
            
            elif row['node'] == 'Email':
                format_msg = self._format_conversation_msg(row['msg'], 300, placeholder, is_email=True)
                if row['source'] == 'Clinic':
                    paragraph += f"{show_date} (BOLD)Klinik skickade ett (COLORRED){row['involved']}(/SPAN) emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){format_msg}(/SPAN)(/ITALIC)\n"
                elif row['source'] == 'Insurance_Company':
                    paragraph += f"{show_date} (BOLD)Försäkringsbolag skickade ett (COLORRED){row['involved']}(/SPAN) emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){format_msg}(/SPAN)(/ITALIC)\n"
                else:
                    paragraph += f"{show_date} (BOLD){row['source']} skickade ett (COLORRED){row['involved']}(/SPAN) emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){format_msg}(/SPAN)(/ITALIC)\n"
            
            elif row['node'] == 'Chat':
                format_msg = self._format_conversation_msg(row['msg'], 300, placeholder)
                paragraph += f"{show_date} (BOLD)(COLORRED){row['involved']}(/SPAN) skickade ett chattmeddelande:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){format_msg}(/SPAN)(/ITALIC)\n"
            
            elif row['node'] == 'Comment':
                format_msg = self._format_conversation_msg(row['msg'], 300, placeholder)
                paragraph += f"{show_date} (BOLD)(COLORRED){row['involved']}(/SPAN) lämnade en kommentar:(/BOLD) \n{placeholder}(ITALIC)(COLORGRAY){format_msg}(/SPAN)(/ITALIC)\n"
            
            elif row['node'] == 'Vet_Fee':
                paragraph += f"{show_date} (BOLD)En veterinäravgift på (COLORRED){row['msg']}(/SPAN) skapades för klinik (COLORRED){row['involved']}(/SPAN).(/BOLD)\n"
                discrepancy += abs(row['source'])
                
            elif row['node'] == 'Create_Invoice':
                paragraph += f"{show_date} (BOLD)En (COLORRED){row['involved']}(/SPAN) faktura skapades för (COLORRED){row['msg']}(/SPAN).(/BOLD)\n"
            
            elif row['node'] == 'Receive_Payment_From_FB':
                paragraph += f"{show_date} (BOLD)Mottog betalning på (COLORRED){row['msg']}(/SPAN) från försäkringsbolag((COLORRED){row['involved']}(/SPAN)).(/BOLD)\n"
                discrepancy -= abs(row['source'])

            elif row['node'] == 'Receive_Payment_From_DÄ':
                paragraph += f"{show_date} (BOLD)Mottog betalning på (COLORRED){row['msg']}(/SPAN) från djurägare((COLORRED){row['involved']}(/SPAN)).(/BOLD)\n"
                discrepancy -= abs(row['source'])

            elif row['node'] == 'Pay_Out_To_CLinic':
                paragraph += f"{show_date} (BOLD)Betalade (COLORRED){row['msg']}(/SPAN) till klinik (COLORRED){row['involved']}(/SPAN).(/BOLD)\n"
            
            elif row['node'] == 'Pay_Back_To_Customer':
                paragraph += f"{show_date} (BOLD)Återbetalade (COLORRED){row['msg']}(/SPAN) till djurägare (COLORRED){row['involved']}(/SPAN).(/BOLD)\n"
                discrepancy -= abs(row['source'])
            
            elif row['node'] == 'Errand_Cancellation':
                paragraph += f"{show_date} (BOLD)Direktregleringsärendet avslutades.(/BOLD)\n"
            
            elif row['node'] == 'Errand_Cancellation_Reversed':
                paragraph += f"{show_date} (BOLD)Direktregleringsärendet återaktiverades.(/BOLD)\n"
        
        return paragraph, discrepancy
    
    def generate_risk_assessment(self, doc_text: str, errand_complete: bool, completed_nodes: Optional[List[str]] = None) -> str:
        """Generate AI risk assessment with automatic model switching on rate limits"""
        if not self.groq_client:
            return "Risk assessment unavailable: No AI client configured"

        arenden_status = "Ja" if errand_complete else "Nej"
        conditional_sections = "" if errand_complete else "- Saknade steg: [List missing steps or write \"Inga\"]\n- Nästa steg: [List next steps or write \"Inga\"]\n"
        system_prompt = self.system_prompt.format(
            BASIC_STEPS=", ".join(self.basic_steps),
            TIME_RULES=", ".join(self.time_rules),
            PAYMENT_RULES=", ".join(self.payment_rules),
            LOGICAL_RULES=", ".join(self.logical_rules),
            COMPLETE=errand_complete,
            ARENDEN_STATUS=arenden_status,
            CONDITIONAL_SECTIONS=conditional_sections
        ).replace('§model_name§', self.model)

        user_prompt = f"Analyze the following errand log and response in Swedish: {doc_text}"
        messages = [
            {"role": "system", "content": system_prompt.replace('§model_name§', self.model)},
            {"role": "user", "content": user_prompt}
        ]
        
        try:
            ai_response, used_model = groq_chat_with_fallback(self.groq_client, messages, self.model)

            if used_model != self.model:
                self.model = used_model

            # Extract header before removing thinking part
            header = None
            for line in ai_response.split('\n'):
                line = line.strip()
                if line.startswith('*') and 'Sammanfattning och Analys' in line:
                    header = line
                    break

            formatted_ai_response = skip_thinking_part(used_model, ai_response)
            return self._normalize_ai_response(formatted_ai_response, errand_complete, completed_nodes, header)
            
        except Exception as e:
            return f"Risk assessment error: {str(e)}"

    def _normalize_ai_response(self, ai_response: str, errand_complete: bool, completed_nodes: Optional[List[str]] = None, header: Optional[str] = None) -> str:
        """
        Normalize log analysis output to ensure consistent formatting across different models.

        Args:
            content: Raw AI response content
            model: Model name used for generation
            errand_complete: Actual completion status of the errand
            log_content: The original log content containing node information

        Returns:
            str: Normalized content with consistent formatting
        """
        # Efficient parsing using split
        lines = ai_response.strip().split('\n')
        fields = {}
        risk_details = []
        collecting_risk_details = False

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Check if this is a field line: "- FieldName: Value"
            if line.startswith('- ') and ':' in line:
                field_part, value_part = line[2:].split(':', 1)
                field_key = field_part.strip().lower()
                field_value = value_part.strip()
                fields[field_key] = field_value

                # Start collecting details if this is risk level
                collecting_risk_details = (field_key == 'risknivå')

            # Collect risk details (bullet points after risk level)
            elif collecting_risk_details and (line.startswith('•') or line.startswith('- ')):
                risk_details.append(line)
            else:
                collecting_risk_details = False

        # Build normalized response starting with header
        normalized_lines = []
        if header:
            normalized_lines.append(header)

        arenden_status = "Ja" if errand_complete else "Nej"
        normalized_lines.append(f"- Ärenden avslutade: {arenden_status}")

        if arenden_status == 'Nej':
            # Analyze the log nodes to determine missing and next steps
            saknade_steg = self._analyze_missing_steps_from_nodes(completed_nodes)
            nasta_steg = self._analyze_next_steps_from_nodes(completed_nodes)

            normalized_lines.append(f"- Saknade steg: {saknade_steg}")
            normalized_lines.append(f"- Nästa steg: {nasta_steg}")

        # Add other fields
        normalized_lines.append(f"- Tidsproblem: {fields.get('tidsproblem', 'Inga')}")
        normalized_lines.append(f"- Betalningsavvikelser: {fields.get('betalningsavvikelser', 'Inga')}")
        normalized_lines.append(f"- Undantag: {fields.get('undantag', 'Inga')}")
        normalized_lines.append(f"- Särskilda riskvarningar: {fields.get('särskilda riskvarningar', 'Inga')}")
        normalized_lines.append(f"- Anledning till avbokning: {fields.get('anledning till avbokning', 'Inga')}")

        # Normalize and preserve risk level with details
        risk_level = self._normalize_risk_level(fields.get('risknivå', 'Låg'))
        normalized_lines.append(f"- Risknivå: {risk_level}")

        # Add risk details if available
        for detail in risk_details:
            normalized_lines.append(detail)

        result = '\n'.join(normalized_lines)
        return result

    def _normalize_risk_level(self, risk_text: str) -> str:
        """Normalize risk level text to standard format"""
        # Extract only the first word (risk level), ignore details
        risk_word = risk_text.split()[0].lower() if risk_text.split() else ''

        if risk_word in ['låg', 'low']:
            return ' Låg'
        elif risk_word in ['medel', 'medium']:
            return ' Medel'
        elif risk_word in ['hög', 'high']:
            return ' Hög'
        else:
            return ' Låg'

    def _get_risk_color(self, risk_text: str) -> str:
        """Get color for risk level (consolidated logic)"""
        risk_lower = risk_text.lower()
        if 'låg' in risk_lower or 'low' in risk_lower:
            return 'green'
        elif 'medel' in risk_lower or 'medium' in risk_lower:
            return 'orange'
        elif 'hög' in risk_lower or 'high' in risk_lower:
            return 'red'
        else:
            return 'black'

    def _analyze_missing_steps_from_nodes(self, completed_nodes: Optional[List[str]] = None) -> str:
        """Analyze the completed nodes to determine which required steps are missing"""
        if completed_nodes is None:
            completed_nodes = []

        present_nodes = set(completed_nodes)

        if "Update_DR" in present_nodes:
            present_nodes.add("Send_To_IC") 

        missing_count = 0
        for step_key in self.required_steps.keys():
            if step_key not in present_nodes:
                missing_count += 1

        return str(missing_count) if missing_count > 0 else "Inga"

    def _analyze_next_steps_from_nodes(self, completed_nodes: Optional[List[str]] = None) -> str:
        """Analyze the completed nodes to determine the next required step"""
        if completed_nodes is None:
            completed_nodes = []

        present_nodes = set(completed_nodes)

        if "Update_DR" in present_nodes:
            present_nodes.add("Send_To_IC")  # Update_DR implies Send_To_IC must have happened

        for step_key, swedish_next_action in self.required_steps.items():
            if step_key not in present_nodes:
                return swedish_next_action

        return "Avsluta ärendet"

    def _format_conversation_msg(self, msg: str, max_length: int, placeholder: str, is_email: bool = False) -> str:
        """
        Clean and format content for email, chat, and comment nodes.
        
        Args:
            content: Raw content string
            max_length: Maximum length before truncation
            placeholder: Placeholder string for indentation
            is_email: Whether this is email content (applies email-specific cleaning)
            
        Returns:
            Cleaned and formatted content string
        """

        clean_msg = str(msg).strip()
        clean_msg = re.sub(r'\n\s*\n\s*\n+', '\n\n', clean_msg)
        clean_msg = re.sub(r'[ \t]+', ' ', clean_msg)
        
        # Email-specific cleaning
        if is_email:
            clean_msg = re.sub(r'\[SUBJECT\]\s*\n\s*', '[SUBJECT]', clean_msg)
            clean_msg = re.sub(r'\[BODY\]\s*\n\s*\n\s*', '[BODY] ', clean_msg)
            clean_msg = re.sub(r'\n\s*\n+', '\n', clean_msg)
            clean_msg = re.sub(r'\s*\n\s*$', '', clean_msg)
        
        if len(clean_msg) > max_length:
            clean_msg = clean_msg[:max_length] + '...'
        
        format_msg = re.sub(r'\n', f'\n{placeholder}', clean_msg)
        format_msg = re.sub(r'\n+$', '', format_msg)
        
        return format_msg
    
    def _format_for_html(self, text: str) -> str:
        """Format text for HTML display"""
        formatted = (text.replace('(COLORBLUE)', '<span style="color:blue;">')
                        .replace('(COLORGRAY)', '<span style="color:gray;">')
                        .replace('(COLORRED)', '<span style="color:red;">')
                        .replace('(ITALIC)', '<i>')
                        .replace('(/ITALIC)', '</i>')
                        .replace('(/SPAN)', '</span>')
                        .replace('(BOLD)', '<b>')
                        .replace('(/BOLD)', '</b>'))
        
        formatted = re.sub(r'[\t\r\n]+', '<br>', formatted)
        # Remove excessive consecutive <br> tags (max 2 in a row)
        formatted = re.sub(r'(<br>){3,}', '<br><br>', formatted)
        formatted = re.sub(r'(<br>€€€€€€€€€€€)+', '<br>€€€€€€€€€€€', formatted)
        formatted = (formatted.replace('<br>€€€€€€€€€€€</span></i><br>', '</span></i><br>')
                            .replace('€', '&nbsp;')
                            .replace('<p><br></p>', '')
                            .replace('<br><br><br>', '<br><br>'))
        
        return formatted
    
    def _clean_before_feed_in_model(self, text: str) -> str:
        """Clean text for AI analysis"""
        clean = (text.replace('€', '')
                    .replace('(COLORBLUE)', '')
                    .replace('(COLORGRAY)', '')
                    .replace('(ITALIC)', '')
                    .replace('(/ITALIC)', '')
                    .replace('(/SPAN)', '')
                    .replace('(BOLD)', '')
                    .replace('(/BOLD)', ''))
        return clean
    
    def _format_ai_analysis(self, ai_text: str) -> str:
        """Format AI analysis for HTML display"""
        def format_risk_level(match):
            risk_line = match.group(1)
            rest_content = match.group(2)

            # Use consolidated color logic
            color = self._get_risk_color(risk_line)

            return (f'<span style="color:{color}; font-weight:bold;">{risk_line}</span><br>'
                   f'<ul style="margin-left: 5px;">'+ ''
                   .join(f'{line.strip()}\n' for line in rest_content.split("\n") if line.strip())
                   + '</ul>')
        
        
        # Remove markdown code block formatting
        formatted = re.sub(r'```\w*\n?', '', ai_text)
        formatted = re.sub(r'```\n?', '', formatted)
        
        # Apply risk level formatting using consolidated logic
        formatted = re.sub(
            r'(- Risknivå: .*?)(\n|$)',
            lambda m: f'<span style="color:{self._get_risk_color(m.group(1))}; font-weight:bold;">{m.group(1)}</span>{m.group(2)}',
            formatted)
        
        # Also handle the old format with bullet points
        formatted = re.sub(
            r'(- Risknivå: .*?)\n+((?:\s*.*\n?)+)',
            format_risk_level, formatted)
        
        formatted = re.sub(r'[\t\r\n]+', '<br>', formatted)
        formatted = re.sub(r'(\*+\sSammanfattning och Analys\s.*?\s*\*+)',
                           r'<div style="text-align: center;">\1</div>',
                           formatted)
        formatted = formatted.replace('<p><br></p>', '')
        
        return formatted
    
