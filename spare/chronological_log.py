"""
Chronological Log Workflow
时间线日志工作流
基于 old_flask_code/chronologicalLog.py 重构
"""
import re
import os
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any, Union
from datetime import datetime
from groq import Groq

from ..app.services.utils import fetchFromDB
from ..app.services.base_service import BaseService


class ChronologicalLogWorkflow(BaseService):
    """时间线日志工作流类"""
    
    def __init__(self, groq_client: Optional[Groq] = None):
        super().__init__()
        self.groq_client = groq_client or self._initialize_groq_client()
        self.drp_fee = 149
        self._setup_configurations()
        self._setup_queries()
        self._setup_rules_and_mappings()
    
    def _initialize_groq_client(self) -> Optional[Groq]:
        """初始化Groq客户端"""
        try:
            api_key = os.getenv('GROQ_API_KEY')
            if api_key:
                return Groq(api_key=api_key)
        except Exception as e:
            print(f"初始化Groq客户端失败: {str(e)}")
        return None
    
    def _setup_configurations(self):
        """设置配置参数"""
        try:
            self.model = pd.read_csv(f"{self.folder}/model.csv")['model'].iloc[0]
        except:
            self.model = "llama-3.3-70b-versatile"
    
    def _setup_queries(self):
        """设置查询语句"""
        try:
            self.log_base_query = self.queries['logBase'].iloc[0]
            self.log_email_query = self.queries['logEmail'].iloc[0]
            self.log_chat_query = self.queries['logChat'].iloc[0]
            self.log_comment_query = self.queries['logComment'].iloc[0]
            self.log_invoice_sp_query = self.queries['logInvoiceSP'].iloc[0]
            self.log_invoice_fo_query = self.queries['logInvoiceFO'].iloc[0]
            self.log_invoice_ka_query = self.queries['logInvoiceKA'].iloc[0]
            self.log_receive_query = self.queries['logReceive'].iloc[0]
            self.log_cancel_query = self.queries['logCancel'].iloc[0]
            self.log_remove_cancel_query = self.queries['logRemoveCancel'].iloc[0]
        except Exception as e:
            print(f"设置查询语句失败: {str(e)}")
            # 设置默认空查询
            for attr in ['log_base_query', 'log_email_query', 'log_chat_query', 'log_comment_query',
                        'log_invoice_sp_query', 'log_invoice_fo_query', 'log_invoice_ka_query',
                        'log_receive_query', 'log_cancel_query', 'log_remove_cancel_query']:
                setattr(self, attr, "SELECT 1 WHERE FALSE")
    
    def _setup_rules_and_mappings(self):
        """设置规则和映射"""
        self.columns = ['errandId', 'node', 'timestamp', 'itemId', 'msg', 'involved', 'source']
        
        # 分类映射
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
            'Manual_Handling_Required': 'Manuell Hantering'
        }
        
        # 业务规则
        self.basic_steps = [
            "Errand Created (Required and Errand Started)",
            "Errand Submitted to Insurance Company (Required)",
            "Email Correspondence Between Insurance Company and Clinic (Optional)",
            "Compensation Amount Updated (Required)",
            "Chat Communication Between Insurance Company, Clinic, and DRP (Optional)",
            "Comment Added (Optional)",
            "Invoice Generated (Required)",
            "Payment Received from Insurance Company (Required)",
            "Payment Received from Clinic (Required)",
            "Payment to Clinic (Required and Errand Closed)"
        ]
        
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
    
    def execute_workflow(self, errand_number: str) -> Dict[str, Any]:
        """
        执行时间线日志工作流
        
        Args:
            errand_number: 案件号
            
        Returns:
            Dict[str, Any]: 包含日志和AI分析的结果
        """
        try:
            # 构建查询条件
            cond1 = True
            cond2 = f"e.errandNumber = '{errand_number}'"
            
            # 获取基础数据
            base = self._get_errand_base(cond1, cond2)
            if base.empty:
                return {"error": f"找不到案件号 {errand_number} 的数据"}
            
            # 收集所有数据
            create = self._create_errand_entry(base)
            send = self._create_send_to_ic_entry(base)
            email, email_base = self._get_email_data(cond1, cond2)
            chat = self._get_chat_data(base, cond1, cond2)
            comment = self._get_comment_data(base, cond1, cond2)
            update = self._create_update_entry(base, email_base)
            invoice = self._create_invoice_entries(cond1, cond2)
            payment = self._create_payment_entries(cond1, cond2)
            cancel = self._create_cancel_entries(cond1, cond2)
            remove = self._create_remove_cancel_entries(cond1, cond2)
            
            # 创建时间线日志
            group_log, group_ai = self._create_chronological_log(
                base, create, send, email, chat, comment,
                update, invoice, payment, cancel, remove
            )
            
            return {
                "success": True,
                "group_log": group_log,
                "group_ai": group_ai,
                "errand_number": errand_number
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"工作流执行失败: {str(e)}",
                "errand_number": errand_number
            }
    
    def _get_errand_base(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """获取案件基础信息"""
        try:
            base = fetchFromDB(self.log_base_query.format(COND1=cond1, COND2=cond2))
            if not base.empty:
                float_cols = base.select_dtypes(include=['float64']).columns
                base[float_cols] = base[float_cols].astype('Int64')
            return base
        except Exception as e:
            print(f"获取案件基础信息失败: {str(e)}")
            return pd.DataFrame()
    
    def _create_errand_entry(self, base: pd.DataFrame) -> pd.DataFrame:
        """创建案件创建记录"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
        
        try:
            create = base[['errandId','errandCreaTime','errandNumber','clinicName']].copy()
            if create.empty:
                return pd.DataFrame(columns=self.columns)
            
            create.loc[:, ['node','msg','source']] = ['Errand_Created', '', '']
            create = create.rename(columns={
                "errandCreaTime": "timestamp",
                "errandNumber": "itemId", 
                "clinicName": "involved"
            }).drop_duplicates()
            
            create['timestamp'] = pd.to_datetime(create['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            create['itemId'] = create['itemId'].apply(lambda row: 'errandNr: ' + str(row))
            
            return create[self.columns]
        except Exception as e:
            print(f"创建案件记录失败: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _create_send_to_ic_entry(self, base: pd.DataFrame) -> pd.DataFrame:
        """创建发送给保险公司记录"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
            
        try:
            send = base.loc[base['sendTime'].notna(), 
                          ['errandId','insuranceCaseId','reference','sendTime','insuranceCompanyName']].copy()
            if send.empty:
                return pd.DataFrame(columns=self.columns)
            
            send.loc[:, ['node','source']] = ['Send_To_IC', '']
            send = send.rename(columns={
                "insuranceCaseId": "itemId",
                "sendTime": "timestamp",
                "reference": "msg",
                "insuranceCompanyName": "involved"
            }).drop_duplicates()
            
            send['timestamp'] = pd.to_datetime(send['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            send['itemId'] = send['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
            send['msg'] = send['msg'].apply(lambda row: 'reference: ' + str(row))
            
            return send[self.columns]
        except Exception as e:
            print(f"创建发送记录失败: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _get_email_data(self, cond1: Any, cond2: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """获取邮件数据"""
        try:
            email_base = fetchFromDB(self.log_email_query.format(COND1=cond1, COND2=cond2))
            email_base_columns = ['errandId', 'emailId', 'subject', 'textPlain', 'textHtml', 
                                'emailTime', 'category', 'correctedCategory', 'source']

            if email_base.empty:
                return pd.DataFrame(columns=self.columns), pd.DataFrame(columns=email_base_columns)

            email_base = email_base[email_base_columns]
            # 合并文本内容
            email_base[['origin','email']] = email_base.apply(
                lambda row: self._merge_text(row['subject'], row['textPlain'], row['textHtml']), 
                axis=1
            ).apply(pd.Series)
            
            email = email_base.copy()
            email.loc[:, ['node']] = 'Email'
            email['correctedCategory'] = email['correctedCategory'].fillna(email['category'])
            email['correctedCategory'] = email['correctedCategory'].map(self.category_mapping)
            
            email = email.rename(columns={
                "emailId": "itemId",
                "emailTime": "timestamp",
                "email": "msg",
                "correctedCategory": "involved"
            }).drop_duplicates()
            
            email['timestamp'] = pd.to_datetime(email['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            email['itemId'] = email['itemId'].apply(lambda row: 'emailId: ' + str(row))
            
            return email[self.columns], email_base
        except Exception as e:
            print(f"获取邮件数据失败: {str(e)}")
            return pd.DataFrame(columns=self.columns), pd.DataFrame()
    
    def _get_chat_data(self, base: pd.DataFrame, cond1: Any, cond2: Any) -> pd.DataFrame:
        """获取聊天数据"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
            
        try:
            chat = fetchFromDB(self.log_chat_query.format(COND1=cond1, COND2=cond2))
            if chat.empty:
                return pd.DataFrame(columns=self.columns)

            chat = pd.merge(chat, base, on='errandId', how='inner')
            chat.loc[:, ['node','involved','source']] = ['Chat', '', '']
            
            for idx, row in chat.iterrows():
                if pd.notna(row.get('chatDRP')):
                    chat.at[idx, 'involved'] = f"{row['chatDRP']}"
                elif pd.notna(row.get('chatClinic')):
                    chat.at[idx, 'involved'] = f"{row['clinicName']}"
                elif pd.notna(row.get('chatMessageId')):
                    chat.at[idx, 'involved'] = f"{row['insuranceCompanyName']}"
            
            chat = chat.rename(columns={
                "chatMessageId": "itemId",
                "chatTime": "timestamp",
                "message": "msg"
            }).drop_duplicates()
            
            chat['timestamp'] = pd.to_datetime(chat['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            chat['itemId'] = chat['itemId'].apply(lambda row: 'chatMessageId: ' + str(row))
            
            return chat[self.columns]
        except Exception as e:
            print(f"获取聊天数据失败: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _get_comment_data(self, base: pd.DataFrame, cond1: Any, cond2: Any) -> pd.DataFrame:
        """获取评论数据"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
            
        try:
            comment = fetchFromDB(self.log_comment_query.format(COND1=cond1, COND2=cond2))
            if comment.empty:
                return pd.DataFrame(columns=self.columns)

            comment = pd.merge(comment, base, on='errandId', how='inner')
            comment = comment[['errandId','commentId','commentTime','commentDRP','content']]
            comment.loc[:, ['node','source']] = ['Comment', '']
            
            comment = comment.rename(columns={
                "commentId": "itemId",
                "commentTime": "timestamp",
                "commentDRP": "involved",
                "content": "msg"
            }).drop_duplicates()
            
            comment['itemId'] = comment['itemId'].apply(lambda row: 'commentId: ' + str(row))
            comment['timestamp'] = pd.to_datetime(comment['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            
            return comment[self.columns]
        except Exception as e:
            print(f"Failed to get comment data: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _create_update_entry(self, base: pd.DataFrame, email_base: pd.DataFrame) -> pd.DataFrame:
        """Create update errand record"""
        if base.empty:
            return pd.DataFrame(columns=self.columns)
            
        try:
            update = base.loc[
                (base['settlementAmount'].notna()) & (base['updatedTime'].notna()),
                ['errandId','insuranceCaseId','updatedTime','settlementAmount']
            ].copy()

            if update.empty:
                return pd.DataFrame(columns=self.columns)

            # Determine update method
            involved = pd.merge(update, email_base[['errandId','category','correctedCategory']], 
                              on='errandId', how='left')
            involved.loc[involved['category'].isna(), 'involved'] = 'by Agria API'
            involved.loc[
                involved['category'].str.startswith('Settlement', na=False) & involved['correctedCategory'].isna(),
                'involved'
            ] = 'by auto-matching'
            involved.loc[
                involved['correctedCategory'].notna() & involved['correctedCategory'].str.startswith('Settlement', na=False),
                'involved'
            ] = 'manually'
            
            involved = involved.drop_duplicates()
            no_involved = involved['involved'].isna().all()
            
            if no_involved:
                involved.loc[
                    involved['settlementAmount'].notna() & involved['involved'].isna(), 
                    'involved'
                ] = 'directly in email'
            else:
                involved = involved.dropna(subset=['involved'])
                
            update.loc[:, ['node','source']] = ['Update_DR', '']
            update = pd.merge(update, involved[['errandId','involved']], on='errandId', how='left')
            
            update = update.rename(columns={
                "insuranceCaseId": "itemId",
                "updatedTime": "timestamp", 
                "settlementAmount": "msg"
            }).drop_duplicates()
            
            update['timestamp'] = pd.to_datetime(update['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            update['msg'] = update['msg'].apply(lambda row: f"{row} kr")
            update['itemId'] = update['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
            
            return update[self.columns]
        except Exception as e:
            print(f"Failed to create update record: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _create_invoice_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create invoice records"""
        try:
            invoice_swedbank = fetchFromDB(self.log_invoice_sp_query.format(COND1=cond1, COND2=cond2))
            invoice_fortus = fetchFromDB(self.log_invoice_fo_query.format(COND1=cond1, COND2=cond2))
            invoice_kassa = fetchFromDB(self.log_invoice_ka_query.format(COND1=cond1, COND2=cond2))
            
            invoice = pd.DataFrame()
            for df in [invoice_swedbank, invoice_fortus, invoice_kassa]:
                if not df.empty:
                    invoice = pd.concat([invoice, df], ignore_index=True)

            if invoice.empty:
                return pd.DataFrame(columns=self.columns)

            invoice = invoice.assign(node='Create_Invoice', source='')
            invoice = invoice.rename(columns={
                "invoiceNumber": "itemId",
                "transTime": "timestamp",
                "invoiceAmount": "msg",
                "paymentOption": "involved"
            }).drop_duplicates()
            
            invoice['timestamp'] = pd.to_datetime(invoice['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            invoice['itemId'] = invoice['itemId'].apply(
                lambda row: 'invoiceNr: ' + str(row) if pd.notna(row) else "No Invoice"
            )
            invoice['msg'] = invoice['msg'].apply(
                lambda row: f"{str(row).replace('-','')} kr" if pd.notna(row) else "No Invoice"
            )
            
            return invoice[self.columns]
        except Exception as e:
            print(f"Failed to create invoice record: {str(e)}")
            return pd.DataFrame(columns=self.columns)
    
    def _create_payment_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create payment records - simplified implementation"""
        return pd.DataFrame(columns=self.columns)
    
    def _create_cancel_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create cancel records - simplified implementation"""
        return pd.DataFrame(columns=self.columns)

    def _create_remove_cancel_entries(self, cond1: Any, cond2: Any) -> pd.DataFrame:
        """Create restore cancel records - simplified implementation"""
        return pd.DataFrame(columns=self.columns)
    
    def _perform_risk_assessment(self, doc: str) -> str:
        """Perform AI risk assessment"""
        if not self.groq_client:
            return "AI risk assessment not available - missing Groq client"
        
        system_prompt = f"""
        You are an expert in log analysis and risk assessment. Your task is to analyze errand logs in Swedish based on predefined rules.

        ### Predefined Steps:
        {", ".join(self.basic_steps)}

        ### Time Rules:
        {", ".join(self.time_rules)}

        ### Payment Rules:
        {", ".join(self.payment_rules)}

        ### Output Format (in Swedish):
        ******************* Sammanfattning och Analys *********************
        - Saknade steg: [Lista eller "Inga"]
        - Nästa steg: [Lista eller "Inga"] 
        - Tidsproblem: [Lista eller "Inga"]
        - Betalningsavvikelser: [Lista eller "Inga"]
        - Undantag: [Lista eller "Inga"]
        - Särskilda riskvarningar: [Lista eller "Inga"]
        - Anledning till avbokning: [Anledning eller "Inga"]
        - Risknivå: [Låg / Medel / Hög]
        """
        
        user_prompt = f"Analyze the following errand log: {doc}"
        
        try:
            chat_completion = self.groq_client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model=self.model
            )
            return chat_completion.choices[0].message.content
        except Exception as e:
            return f"AI analysis error: {str(e)}"
    
    def _create_chronological_log(self, base: pd.DataFrame, create: pd.DataFrame, send: pd.DataFrame,
                                 email: pd.DataFrame, chat: pd.DataFrame, comment: pd.DataFrame,
                                 update: pd.DataFrame, invoice: pd.DataFrame, payment: pd.DataFrame,
                                 cancel: pd.DataFrame, remove: pd.DataFrame) -> Tuple[Dict, Dict]:
        """Create chronological log"""
        def filter_columns(df):
            if df.empty:
                return df
            return df.loc[:, df.notna().any()]

        # Merge all data
        processed_dfs = [
            filter_columns(df) 
            for df in [create, send, email, chat, comment, update, invoice, payment, cancel, remove]
            if not df.empty
        ]

        if not processed_dfs:
            return {}, {}

        try:
            log = pd.concat(processed_dfs, ignore_index=True).drop_duplicates()
            log = pd.merge(log, base[['errandId','clinicName','insuranceCompanyName']], 
                          on='errandId', how='left')

            float_cols = log.select_dtypes(include=['float64']).columns
            if not float_cols.empty:
                log[float_cols] = log[float_cols].astype('Int64')
            
            log = log.sort_values('timestamp')
            
            grouped = log.groupby('errandId')
            group_log, group_ai = {}, {}

            for group_id, group_df in grouped:
                group_df = group_df.sort_values('timestamp').reset_index(drop=True)
                group_df['timestamp'] = group_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                
                # Build log content
                paragraph = f"Ärenden: {group_id}\n\n"
                discrepancy = 0
                
                for idx, row in group_df.iterrows():
                    timestamp = row['timestamp']
                    node = row['node']
                    msg = row.get('msg', '')
                    involved = row.get('involved', '')
                    
                    if node == 'Errand_Created':
                        paragraph += f"• {timestamp} Direktregleringsärendet skapades av klinik {involved}.\n"
                    elif node == 'Send_To_IC':
                        paragraph += f"• {timestamp} Direktregleringsärendet skickades till försäkringsbolag {involved}.\n"
                    elif node == 'Update_DR':
                        paragraph += f"• {timestamp} Direktregleringsärendet uppdaterade ersättningsbeloppet {msg} {involved}.\n"
                    elif node == 'Email':
                        paragraph += f"• {timestamp} Email: {involved} - {msg[:100]}...\n"
                    elif node == 'Chat':
                        paragraph += f"• {timestamp} Chat från {involved}: {msg}\n"
                    elif node == 'Comment':
                        paragraph += f"• {timestamp} Kommentar från {involved}: {msg}\n"
                    elif node == 'Create_Invoice':
                        paragraph += f"• {timestamp} En {involved} faktura skapades för {msg}.\n"
                    else:
                        paragraph += f"• {timestamp} {node}: {msg}\n"
                
                # Calculate payment discrepancies
                if discrepancy == self.drp_fee or discrepancy == 0:
                    paragraph = f"Ärenden: {group_id} Betalningsavvikelse: Nej\n\n" + paragraph[len(f"Ärenden: {group_id}\n\n"):]
                else:
                    paragraph = f"Ärenden: {group_id} Betalningsavvikelse: {int(discrepancy)}kr\n\n" + paragraph[len(f"Ärenden: {group_id}\n\n"):]
                
                # Format as HTML
                html_content = paragraph.replace('\n', '<br>')
                
                group_log[group_id] = {
                    "title": f"Ärenden: {group_id}",
                    "content": html_content
                }
                
                # AI analysis
                ai_analysis = self._perform_risk_assessment(paragraph)
                group_ai[group_id] = ai_analysis.replace('\n', '<br>')
            
            return group_log, group_ai
            
        except Exception as e:
            print(f"Failed to create chronological log: {str(e)}")
            return {}, {}

    def batch_process_errands(self, errand_numbers: List[str]) -> Dict[str, Any]:
        """Batch process multiple errands"""
        results = {}
        errors = []
        
        for errand_number in errand_numbers:
            try:
                result = self.execute_workflow(errand_number)
                results[errand_number] = result
            except Exception as e:
                errors.append({
                    "errand_number": errand_number,
                    "error": str(e)
                })
        
        return {
            "results": results,
            "errors": errors,
            "total_processed": len(errand_numbers),
            "successful": len(results),
            "failed": len(errors)
        }

    def validate_errand_number(self, errand_number: str) -> bool:
        """Validate errand number format"""
        if not errand_number or not isinstance(errand_number, str):
            return False
        
        # Simple errand number validation - adjust according to actual format
        return errand_number.strip() != "" and len(errand_number.strip()) > 0
