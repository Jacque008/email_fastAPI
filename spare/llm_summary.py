"""
LLM Summary Workflow
LLM summary workflow
Refactored based on old_flask_code/llmSummary.py
"""
import re
import os
import groq
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
from groq import Groq

from ..app.services.utils import fetchFromDB
from ..app.services.base_service import BaseService


class LLMSummaryWorkflow(BaseService):
    """LLM summary workflow class"""
    
    def __init__(self, groq_client: Optional[Groq] = None):
        super().__init__()
        self.groq_client = groq_client or self._initialize_groq_client()
        self._setup_summary_configurations()
        self._setup_summary_queries()
        self.system_prompt = self._get_system_prompt()
    
    def _initialize_groq_client(self) -> Optional[Groq]:
        """Initialize Groq client"""
        try:
            api_key = os.getenv('GROQ_API_KEY')
            if api_key:
                return Groq(api_key=api_key)
        except Exception as e:
            print(f"Failed to initialize Groq client: {str(e)}")
        return None
    
    def _setup_summary_configurations(self):
        """Setup summary configurations"""
        try:
            self.model = pd.read_csv(f"{self.folder}/model.csv")['model'].iloc[0]
        except:
            self.model = "llama-3.3-70b-versatile"
    
    def _setup_summary_queries(self):
        """Setup summary query statements"""
        try:
            self.summary_chat_query = self.queries['summaryChat'].iloc[0]
            self.summary_email_query = self.queries['summaryEmail'].iloc[0]
            self.summary_comment_query = self.queries['summaryComment'].iloc[0]
        except Exception as e:
            print(f"Failed to setup summary queries: {str(e)}")
            # Set default queries
            self.summary_chat_query = "SELECT 1 WHERE FALSE"
            self.summary_email_query = "SELECT 1 WHERE FALSE" 
            self.summary_comment_query = "SELECT 1 WHERE FALSE"
    
    def _get_system_prompt(self) -> Dict[str, str]:
        """Get system prompts"""
        return {
            "role": "system",
            "content": (
                "You are a professional and efficient assistant from Sweden, "
                "specializing in accurately summarizing conversations in Swedish. "
                "Your summaries must be clear, concise, accurate, and well-structured. "
                "Format the summary as bullet points, starting each point with '*   '. "
                "Ensure precision and avoid unnecessary information. "
                "Do not translate the text."
                "\n\n"
                "Background information: "
                "- 'Clinic' refers to veterinary clinics that provide medical care for pets. After treating a pet, clinics are responsible for submitting claims to insurance companies, providing accurate and complete information required for reimbursement. "
                "- 'Insurance company' refers to the entity responsible for processing claims submitted by clinics. They request the necessary information, make claim decisions, and ensure timely payments. "
                "- 'DRP' (direktreglering company) acts as an intermediary platform connecting clinics and insurance companies. All communication between clinics and insurance companies occurs via the DRP platform. DRP is also responsible for forwarding emails and handling payments: insurance companies pay DRP, which deducts service fees before forwarding payments to clinics."
            )
        }
    
    def execute_workflow(self, email_id: Optional[int] = None, errand_number: Optional[str] = None, 
                        reference: Optional[str] = None, use_case: str = 'api') -> Dict[str, Any]:
        """
        执行LLM摘要工作流
        
        Args:
            email_id: 邮件ID
            errand_number: 案件号
            reference: 参考号
            use_case: 使用场景 ('api' 或 'webService')
            
        Returns:
            Dict[str, Any]: 摘要结果
        """
        if not self.groq_client:
            return {"error_message": "AI服务不可用 - 缺少Groq客户端"}
        
        try:
            # 构建查询条件
            condition = self._build_query_condition(email_id, errand_number, reference)
            if not condition:
                return {"error_message": "缺少必要参数：emailId、errandNumber或reference"}
            
            # 获取和处理数据
            chat = self._fetch_data('chat', condition['chat'])
            email = self._fetch_data('email', condition['email'])
            comment = self._fetch_data('comment', condition['comment'])
            
            # 处理不同使用场景
            if use_case == 'webService':
                return self._process_web_service_case(chat, email, comment)
            else:
                return self._process_api_case(chat, email, comment)
                
        except Exception as e:
            return {"error_message": f"摘要工作流执行失败: {str(e)}"}
    
    def _build_query_condition(self, email_id: Optional[int], errand_number: Optional[str], 
                              reference: Optional[str]) -> Optional[Dict[str, str]]:
        """构建查询条件"""
        if email_id:
            return {
                'chat': f"e.id = {email_id}",
                'email': f"e.id = {email_id}",
                'comment': f"e.id = {email_id}"
            }
        elif errand_number:
            return {
                'chat': f"e.errandNumber = '{errand_number}'",
                'email': f"e.errandNumber = '{errand_number}'",
                'comment': f"e.errandNumber = '{errand_number}'"
            }
        elif reference:
            return {
                'chat': f"ic.reference = '{reference}'",
                'email': f"ic.reference = '{reference}'",
                'comment': f"ic.reference = '{reference}'"
            }
        return None
    
    def _fetch_data(self, kind: str, condition: str) -> pd.DataFrame:
        """获取数据：聊天记录、邮件或评论"""
        query_mapping = {
            'chat': self.summary_chat_query,
            'email': self.summary_email_query,
            'comment': self.summary_comment_query
        }
        
        if kind not in query_mapping:
            return pd.DataFrame()
        
        try:
            query = query_mapping[kind].format(CONDITION=condition)
            return fetchFromDB(query)
        except Exception as e:
            print(f"获取{kind}数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _process_api_case(self, chat: pd.DataFrame, email: pd.DataFrame, 
                         comment: pd.DataFrame) -> Dict[str, Any]:
        """处理API使用场景"""
        # 合并处理
        combine = self._process_combine(chat, email, comment)
        summary_combine, error_combine = None, None
        
        if not combine.empty and combine['content'].notna().any():
            msg_combine = self._format_combine(combine)
            if msg_combine:
                summary_combine, error_combine = self._get_ai_response(msg_combine)
        else:
            error_combine = "Inga tillgängliga data"
        
        return {
            'summary_combine': summary_combine,
            'error_combine': error_combine
        }
    
    def _process_web_service_case(self, chat: pd.DataFrame, email: pd.DataFrame, 
                                 comment: pd.DataFrame) -> Dict[str, Any]:
        """处理Web服务使用场景"""
        # 处理聊天数据
        summary_clinic, summary_ic, error_chat = None, None, None
        clinic_name, ic_name = None, None
        
        if not chat.empty:
            chat = self._process_chat(chat)
            chat_clinic = chat[(chat['type_'] == 'errand') & (chat['message'].notna())]
            chat_ic = chat[(chat['type_'] == 'insurance_company_errand') & (chat['message'].notna())]
            
            if not chat_clinic.empty and chat_clinic['reference'].notna().any():
                msg_clinic, clinic_name = self._format_chat(chat_clinic)
                if msg_clinic:
                    summary_clinic, error_chat = self._get_ai_response(msg_clinic)
            
            if not chat_ic.empty and chat_ic['reference'].notna().any():
                msg_ic, ic_name = self._format_chat(chat_ic)
                if msg_ic:
                    summary_ic, error_chat = self._get_ai_response(msg_ic)
        else:
            error_chat = 'Ingen Chatt Tillgänglig.'
        
        # 处理邮件数据
        summary_email, error_email = None, None
        sender, recipient = None, None
        
        if not email.empty and email['id'].notna().any():
            email = self._process_email(email)
            msg_email, sender, recipient = self._format_email(email)
            if msg_email:
                summary_email, error_email = self._get_ai_response(msg_email)
        else:
            error_email = 'Ingen Email Tillgänglig.'
        
        # 处理评论数据
        summary_comment_dr, summary_comment_email = None, None
        error_comment_dr, error_comment_email = None, None
        
        if not comment.empty:
            comment = self._process_comment(comment)
            comment_dr = comment[comment['type'] == 'Errand']
            comment_email = comment[comment['type'] == 'Email']
            
            if not comment_dr.empty:
                msg_comment_dr = self._format_comment(comment_dr)
                if msg_comment_dr:
                    summary_comment_dr, error_comment_dr = self._get_ai_response(msg_comment_dr)
            else:
                error_comment_dr = 'Inga Kommentarer Tillgängliga för DR.'
            
            if not comment_email.empty:
                msg_comment_email = self._format_comment(comment_email)
                if msg_comment_email:
                    summary_comment_email, error_comment_email = self._get_ai_response(msg_comment_email)
            else:
                error_comment_email = 'Inga Kommentarer Tillgängliga för Email.'
        else:
            error_comment_dr = 'Inga Kommentarer Tillgängliga för DR.'
            error_comment_email = 'Inga Kommentarer Tillgängliga för Email.'
        
        # 合并处理
        combine = self._process_combine(chat, email, comment)
        summary_combine, error_combine = None, None
        
        if not combine.empty and combine['content'].notna().any():
            msg_combine = self._format_combine(combine)
            if msg_combine:
                summary_combine, error_combine = self._get_ai_response(msg_combine)
        else:
            error_combine = "Inga tillgängliga data"
        
        return {
            'summary_clinic': summary_clinic,
            'clinic_name': clinic_name,
            'summary_ic': summary_ic,
            'ic_name': ic_name,
            'error_chat': error_chat,
            'summary_email': summary_email,
            'sender': sender,
            'recipient': recipient,
            'error_email': error_email,
            'summary_comment_dr': summary_comment_dr,
            'error_comment_dr': error_comment_dr,
            'summary_comment_email': summary_comment_email,
            'error_comment_email': error_comment_email,
            'summary_combine': summary_combine,
            'error_combine': error_combine
        }
    
    def _process_chat(self, chat: pd.DataFrame) -> pd.DataFrame:
        """处理聊天数据"""
        if chat.empty:
            return chat
            
        chat['createdAt'] = pd.to_datetime(chat['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        
        float_cols = chat.select_dtypes(include=['float64']).columns
        chat[float_cols] = chat[float_cols].astype('Int64')
        chat['source'] = None
        
        return chat
    
    def _format_chat(self, chat: pd.DataFrame) -> Tuple[List[Dict], Optional[str]]:
        """格式化聊天记录为AI处理格式"""
        user_prompt = (
            "Summarize the following chat conversation between DRP and {PART} in Swedish. "
            "Do not translate the text. Focus only on the key points of the conversation. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances. "
            "The summary should be concise, accurate, and formatted as bullet points."
            "\n\n{CHAT}")
        
        msg_chat, his_chat, name = [], [], None
        
        if chat['type_'].iloc[0] == 'errand':
            name = chat[chat['clinicName'].notna()].clinicName.iloc[0]
            for idx, row in chat.iterrows(): 
                if pd.notna(row['fromClinicUserId']) and pd.isna(row['fromAdminUserId']):
                    his_chat.append({"role": "clinic", "content": row['message']})
                    chat.at[idx, 'source'] = 'Clinic'
                elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromClinicUserId']):
                    his_chat.append({"role": "DRP", "content": row['message']})
                    chat.at[idx, 'source'] = 'DRP'
            
            msg_chat.append(self.system_prompt)
            msg_chat.append({"role": "user", "content": user_prompt.format(PART='a clinic named ' + name, CHAT=his_chat)})
            
        elif chat['type_'].iloc[0] == 'insurance_company_errand':
            name = chat[chat['insuranceCompanyName'].notna()].insuranceCompanyName.iloc[0]
            for idx, row in chat.iterrows():
                if pd.notna(row['message']):
                    if pd.notna(row['fromInsuranceCompanyId']) and pd.isna(row['fromAdminUserId']):
                        his_chat.append({"role": "insurance company", "content": row['message']})
                        chat.at[idx, 'source'] = 'Insurance_Company'
                    elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromInsuranceCompanyId']):
                        his_chat.append({"role": "DRP", "content": row['message']})
                        chat.at[idx, 'source'] = 'DRP'
            
            msg_chat.append(self.system_prompt)
            msg_chat.append({"role": "user", "content": user_prompt.format(PART='an insurance company named ' + name, CHAT=his_chat)})
        
        return msg_chat, name
    
    def _process_email(self, email: pd.DataFrame) -> pd.DataFrame:
        """处理邮件数据"""
        if email.empty:
            return email
            
        email['createdAt'] = pd.to_datetime(email['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        
        # 处理收件箱邮件
        inbox = email[email['folder']=='inbox'].copy()
        if not inbox.empty:
            pro = self._process_email_text(inbox)
            inbox = pd.merge(
                inbox[['id','createdAt','subject','sender','recipient', 'folder']], 
                pro[['id','source','sendTo','email']], 
                on='id', how='left'
            )
            inbox = inbox[['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder']]
        else:
            inbox = pd.DataFrame(columns=['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder'])
        
        # 处理已发送邮件
        sent = email[email['folder']=='sent'].copy()
        if not sent.empty:
            sent['source'], sent['sender'], sent['sendTo'], sent['recipient'] = 'DRP', 'DRP', 'Other', None
            sent['email'] = sent.apply(lambda row: self._merge_text(row['subject'], row['textPlain'], row['textHtml']), axis=1).str[1]
            sent = sent[['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder']]
        else:
            sent = pd.DataFrame(columns=['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder'])
        
        processed_email = pd.concat([inbox, sent], ignore_index=True)
        if not processed_email.empty:
            processed_email['email'] = processed_email['email'].str.split('[BODY]', n=1).str[1].fillna('')
        
        return processed_email
    
    def _format_email(self, email: pd.DataFrame) -> Tuple[List[Dict], str, str]:
        """格式化邮件为AI处理格式"""
        user_prompt = (
            "Summarize the following email conversation between {SOURCE} and {SENDTO} in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances."
            "\n\n{EMAILTEXT}")
        
        sender, recipient = 'Unknown', 'Unknown'
        not_null = email[(email['email'].notna()) & (email['email'] != '')]
        msg_email, his_email, email_text = [], [], None
        
        if not not_null.empty:
            for _, row in not_null.iterrows():
                his_email.append({
                    "role": row['source'] if pd.notna(row['source']) and (row['source'] in ["Clinic", "Insurance_Company", "DRP"]) else "Other",
                    "content": row['email'].strip()
                })
            
            email_text = "\n".join([f"{item['role']}: {item['content']}" for item in his_email])
            
            first_row = not_null.iloc[0]
            source = (first_row['source'] or '').strip()
            send_to = (first_row['sendTo'] or '').strip()
            
            msg_email.append(self.system_prompt)
            msg_email.append({
                "role": "user", 
                "content": user_prompt.format(SOURCE=source, SENDTO=send_to, EMAILTEXT=email_text)
            })
            
            sender = first_row['sender']
            recipient = first_row['recipient']
        
        return msg_email, sender, recipient
    
    def _process_comment(self, comment: pd.DataFrame) -> pd.DataFrame:
        """处理评论数据"""
        if comment.empty:
            return comment
            
        comment['createdAt'] = pd.to_datetime(comment['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        comment['source'] = 'DRP'
        
        return comment
    
    def _format_comment(self, comment: pd.DataFrame) -> List[Dict]:
        """格式化评论为AI处理格式"""
        user_prompt = (
            "Summarize the following comments concisely and accurately in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Exclude all greeting words, and ensure the output does not contain blank lines or isolated '*'."
            "\n\n{COMMENTS}")
        
        msg_comment, his_comment = [], []
        for _, row in comment.iterrows():
            his_comment.append(row['content'].strip())
        
        msg_comment.append(self.system_prompt)
        msg_comment.append({"role": "user", "content": user_prompt.format(COMMENTS=his_comment)})
        
        return msg_comment
    
    def _process_combine(self, chat: pd.DataFrame, email: pd.DataFrame, comment: pd.DataFrame) -> pd.DataFrame:
        """合并聊天、邮件和评论数据"""
        cols = ['createdAt', 'source', 'content', 'combineType']
        
        # 处理聊天数据
        if not chat.empty:
            chat = chat.rename({'message':'content'}, axis=1)
            chat["combineType"] = "chat"
        else:
            chat = pd.DataFrame(columns=cols)
        
        # 处理邮件数据    
        if not email.empty:
            email = email.rename({'email':'content'}, axis=1)
            email["combineType"] = "email"
        else:
            email = pd.DataFrame(columns=cols)
        
        # 处理评论数据
        if not comment.empty:
            comment["combineType"] = "comment"
        else:
            comment = pd.DataFrame(columns=cols)
        
        # 合并所有非空数据
        no_empty = [df for df in [chat[cols], email[cols], comment[cols]] if not df.empty]
        if no_empty:
            data = pd.concat(no_empty, ignore_index=True).sort_values(by='createdAt', ignore_index=True)
        else:
            data = pd.DataFrame(columns=cols)
        
        return data
    
    def _format_combine(self, combine: pd.DataFrame) -> List[Dict]:
        """格式化合并数据为AI处理格式"""
        user_prompt = (
            "Summarize the following conversation between the clinic, insurance company, and DRP concisely and accurately in Swedish. "
            "Capture only the key points from the email conversations and comments. "
            "Replace 'insurance company' with 'FB' in all instances. "
            "\n\n"
            "Additional instructions: "
            "- If DRP only forwarded an email, do not mention it in the summary to save space. "
            "- A forwarded email is defined as an email where most of the content is identical to the previous email, with only some template text added at the beginning or end. "
            "- If DRP performed any actions beyond forwarding emails, include those actions in the summary. "
            "- Ensure the summary is concise, well-structured, and focuses on key points relevant to the errand."
            "\n\n"
            "{COMBINE}")
        
        msg_combine, his_combine = [], []
        if not combine.empty:
            for _, row in combine.iterrows():
                his_combine.append({
                    "role": row['combineType'] + " from " + row['source'] if pd.notna(row['source']) and (row['source'] in ["Clinic", "Insurance_Company", "DRP"]) else "Other",
                    "content": row['content'].strip()
                })
            
            combine_text = "\n".join([f"{item['role']}: {item['content']}" for item in his_combine])
            
            msg_combine.append(self.system_prompt)
            msg_combine.append({
                "role": "user",
                "content": user_prompt.format(COMBINE=combine_text)
            })
        
        return msg_combine
    
    def _get_ai_response(self, msg: List[Dict]) -> Tuple[Optional[str], Optional[str]]:
        """获取AI响应"""
        summary, error_message = None, None
        try:
            chat_completion = self.groq_client.chat.completions.create(
                messages=msg,
                model=self.model
            )
            summary = chat_completion.choices[0].message.content
            parts = re.split(r':\s*', summary)
            if len(parts) > 1:
                summary = ":".join(parts[1:])
        except groq.APIConnectionError:
            error_message = "The server could not be reached. Please try again later."
        except groq.RateLimitError:
            error_message = "Rate limit exceeded. Please try again later."
        except groq.APIStatusError as e:
            error_message = f"API Error: {e.status_code}, Response: {e.response}"
        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
        
        return summary, error_message
    
    def batch_process_summaries(self, summary_requests: List[Dict[str, Any]], 
                               use_case: str = 'api') -> Dict[str, Any]:
        """批量处理摘要请求"""
        results = []
        errors = []
        
        for i, request in enumerate(summary_requests):
            try:
                result = self.execute_workflow(
                    email_id=request.get('emailId'),
                    errand_number=request.get('errandNumber'),
                    reference=request.get('reference'),
                    use_case=use_case
                )
                results.append(result)
            except Exception as e:
                errors.append({
                    "index": i,
                    "request": request,
                    "error": str(e)
                })
        
        return {
            "results": results,
            "errors": errors,
            "total_processed": len(summary_requests),
            "successful": len(results),
            "failed": len(errors)
        }
    
    def validate_summary_request(self, email_id: Optional[int], errand_number: Optional[str], 
                                reference: Optional[str]) -> Tuple[bool, str]:
        """验证摘要请求"""
        if not email_id and not errand_number and not reference:
            return False, "必须提供emailId、errandNumber或reference中的一个参数"
        
        provided_params = sum([bool(email_id), bool(errand_number), bool(reference)])
        if provided_params > 1:
            return False, "只能提供一个参数：emailId、errandNumber或reference"
        
        if email_id and email_id <= 0:
            return False, "emailId必须是正整数"
        
        if errand_number and not errand_number.strip():
            return False, "errandNumber不能为空"
        
        if reference and not reference.strip():
            return False, "reference不能为空"
        
        return True, "验证通过"
    
    def get_summary_statistics(self, summary_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """获取摘要统计信息"""
        total = len(summary_results)
        successful = len([r for r in summary_results if not r.get('error_message') and not r.get('error_combine')])
        failed = total - successful
        
        # 统计有内容的摘要类型
        chat_summaries = len([r for r in summary_results if r.get('summary_clinic') or r.get('summary_ic')])
        email_summaries = len([r for r in summary_results if r.get('summary_email')])
        comment_summaries = len([r for r in summary_results if r.get('summary_comment_dr') or r.get('summary_comment_email')])
        combined_summaries = len([r for r in summary_results if r.get('summary_combine')])
        
        return {
            "total_processed": total,
            "successful": successful,
            "failed": failed,
            "success_rate": successful / total if total > 0 else 0,
            "summary_types": {
                "chat_summaries": chat_summaries,
                "email_summaries": email_summaries,
                "comment_summaries": comment_summaries,
                "combined_summaries": combined_summaries
            }
        }
