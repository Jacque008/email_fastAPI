import re
import os
import groq
import pandas as pd
from groq import Groq
from .preprocess import PreProcess
from .utils import fetchFromDB

# AI_Summary API

    # URL: https://classify-emails-596633500987.europe-west4.run.app/summary_api

    # Usage: By inputing an errandId, the API retrieves all related email conversations and chat histories associated with the errand. 
    #         Chats may involve interactions between the DR and Clinic or the DR and IC, while email conversations can occur between any two of Clinic, IC, and DRP. 
    #         If both chat and email exist, the API will summarize them together. If only one type exists, the API will summarize the available one

    # Input Data (Only one at a time):
    #     [
    #         {    
    #             "emailId": 123456,
    #             "errandNumber": '',
    #             "reference": ''           # only one valid value allowed!
    #         }
    #     ]

    # Output Data (will be also one at a time):
    #     [
    #         {
    #             "Error_Message": null,
    #             "Summary_Chat_with_Clinic": "",
    #             "Summary_Chat_with_IC": "",
    #             "Summary_Email_Conversation": ""
    #         }
    #     ]
    
class LLMSummary(PreProcess):
    def __init__(self): 
        super().__init__()
        self.summaryChatQuery = self.queries['summaryChat'].iloc[0] # keep for summary and app.py
        self.summaryEmailQuery = self.queries['summaryEmail'].iloc[0] # keep for summary and app.py
        self.summaryCommentQuery = self.queries['summaryComment'].iloc[0] # keep for summary and app.py
        self.model = pd.read_csv(f"{self.folder}/model.csv")['model'].iloc[0] 
        self.systemPrompt = {
            "role": "system",
            "content": (
                "You are a professional and efficient assistant from Sweden, "
                "specializing in accurately summarizing conversations in Swedish. "
                "Your summaries must be clear, concise, accurate, and well-structured. "
                "Format the summary as bullet points, starting each point with '*   '. "
                "Ensure precision and avoid unnecessary information."
                "Do not translate the text."
                "\n\n"
                "Background information: "
                "- 'Clinic' refers to veterinary clinics that provide medical care for pets. After treating a pet, clinics are responsible for submitting claims to insurance companies, providing accurate and complete information required for reimbursement. "
                "- 'Insurance company' refers to the entity responsible for processing claims submitted by clinics. They request the necessary information, make claim decisions, and ensure timely payments. "
                "- 'DRP' (direktreglering company) acts as an intermediary platform connecting clinics and insurance companies. All communication between clinics and insurance companies occurs via the DRP platform. DRP is also responsible for forwarding emails and handling payments: insurance companies pay DRP, which deducts service fees before forwarding payments to clinics.")}

    def _processChat(self, chat):
        chat['createdAt'] = pd.to_datetime(chat['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        
        float_cols = chat.select_dtypes(include=['float64']).columns
        chat[float_cols] = chat[float_cols].astype('Int64')
        chat['source'] = None
        
        return chat
    
    def _formatChat(self, chat):
        userPrompt = (
            "Summarize the following chat conversation between DRP and {PART} in Swedish. "
            "Do not translate the text. Focus only on the key points of the conversation. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances. "
            "The summary should be concise, accurate, and formatted as bullet points."
            "\n\n{CHAT}")
        
        msgChat, hisChat, name = [], [], None
        
        if chat['type_'].iloc[0]=='errand':
            name =  chat[chat['clinicName'].notna()].clinicName.iloc[0]
            for idx, row in chat.iterrows(): 
                if pd.notna(row['fromClinicUserId']) and pd.isna(row['fromAdminUserId']):
                    hisChat.append({"role": "clinic", "content": row['message']})
                    chat.at[idx, 'source'] = 'Clinic'
                elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromClinicUserId']):
                    hisChat.append({"role": "DRP", "content": row['message']})
                    chat.at[idx, 'source'] = 'DRP'
            
            msgChat.append(self.systemPrompt)
            msgChat.append({"role": "user", "content": userPrompt.format(PART='a clinic named ' + name, CHAT=hisChat)})
            
        elif chat['type_'].iloc[0]=='insurance_company_errand':
            name = chat[chat['insuranceCompanyName'].notna()].insuranceCompanyName.iloc[0]
            for idx, row in chat.iterrows():
                if pd.notna(row['message']):
                    if pd.notna(row['fromInsuranceCompanyId']) and pd.isna(row['fromAdminUserId']):
                        hisChat.append({"role": "insurance company", "content": row['message']})
                        chat.at[idx, 'source'] = 'Insurance_Company'
                    elif pd.notna(row['fromAdminUserId']) and pd.isna(row['fromInsuranceCompanyId']):
                        hisChat.append({"role": "DRP", "content": row['message']})
                        chat.at[idx, 'source'] = 'DRP'
            
            msgChat.append(self.systemPrompt)
            msgChat.append({"role": "user", "content": userPrompt.format(PART='an insurance company named ' + name, CHAT=hisChat)})        
        
        return msgChat, name
        
    def _processEmail(self, email):
        email['createdAt'] = pd.to_datetime(email['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        
        inbox = email[email['folder']=='inbox'].copy()      
        pro = self._processEmailText(inbox)
        inbox = pd.merge(inbox[['id','createdAt','subject','sender','recipient', 'folder']], pro[['id','source','sendTo','email']], on='id',how='left')
        inbox = inbox[['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder']]
        
      # process sent out emails
        # source/sender: 'DRP', sendTo: ['Insurance_Company','Clinic','Finance','DRP','Other'], recipient: detail name    
        sent = email[email['folder']=='sent'].copy() 
        sent['source'], sent['sender'], sent['sendTo'], sent['recipient'] = 'DRP', 'DRP', 'Other', None
        
        sent['recipient'] = sent['to'].str.lower().str.extract(f'({self.icRefStr})', expand=False)
        sent.loc[sent['recipient'].notna(), 'sendTo'] = 'Insurance_Company'
        
        mask_toDrp = (sent['sendTo'] == 'Other') & (sent['recipient'].isna()) & (sent['to'].str.lower().str.contains(self.drpStr, na=False))
        sent.loc[mask_toDrp, ['sendTo', 'recipient']] = ['DRP', 'DRP']

        mask_toFinance = (sent['sendTo'] == 'Other') & (sent['recipient'].isna())
        sent.loc[mask_toFinance, 'recipient'] = sent.loc[mask_toFinance, 'to'].str.lower().str.extract(f'(fortus|payex)', expand=False)
        sent.loc[(sent['sendTo'] == 'Other') & (sent['recipient'].notna()), 'sendTo'] = 'Finance'

        mask_toClinic = (sent['sendTo'] == 'Other') & (sent['recipient'].isna())
        clinic_data = pd.merge(sent[mask_toClinic], self.clinicList[['clinicName', 'clinicEmail']], left_on='to', right_on='clinicEmail', how='left')
        sent.loc[mask_toClinic, 'recipient'] = clinic_data['clinicName']
        sent.loc[(sent['sendTo'] == 'Other') & (sent['recipient'].notna()), 'sendTo'] = 'Clinic'
        
        sent['sendTo'] = sent['sendTo'].fillna('Other')

        sent['email'] = sent.apply(lambda row: self._mergeText(row['subject'], row['textPlain'], row['textHtml']),axis=1).str[1]
        sent = sent[['id', 'createdAt', 'source', 'sender', 'sendTo', 'recipient', 'subject', 'email', 'folder']]
        
        processedEmail = pd.concat([inbox, sent], ignore_index=True)
        processedEmail['email'] = processedEmail['email'].str.split('[BODY]', n=1).str[1].fillna('')
        
        return processedEmail
    
    def _formatEmail(self, email): 
        userPrompt = (
            "Summarize the following email conversation between {SOURCE} and {SENDTO} in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Replace the clinic's name with 'kliniken' and the insurance company's name with 'FB' in all instances."
            "\n\n{EMAILTEXT}")
        
        source, sendTo, sender, recipient = None, None, 'Unknown', 'Unknown'
        notNull = email[(email['email'].notna()) & (email['email'] != '')]
        msgEmail, hisEmail, emailText = [], [], None
        if not notNull.empty:
            for _, row in notNull.iterrows():
                hisEmail.append({
                    "role": row['source'] if pd.notna(row['source']) and (row['source'] in ["Clinic", "Insurance_Company", "DRP"]) else "Other",
                    "content": row['email'].strip()})
                
            emailText = "\n".join([f"{item['role']}: {item['content']}" for item in hisEmail])
            
            firstRow = notNull.iloc[0]
            source = (firstRow['source'] or '').strip()
            sendTo = (firstRow['sendTo'] or '').strip()
            
            msgEmail.append(self.systemPrompt)
            msgEmail.append({ "role": "user", "content": userPrompt.format(SOURCE=source, SENDTO=sendTo, EMAILTEXT=emailText)})

            sender = firstRow['sender']
            recipient = firstRow['recipient']
            
        return msgEmail, sender, recipient
    
    def _processComment(self, comment):
        comment['createdAt'] = pd.to_datetime(comment['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
        comment['source'] = 'DRP'
        
        return comment
    
    def _formatComment(self, comment): 
        userPrompt = (
            "Summarize the following comments concisely and accurately in Swedish. "
            "Provide a concise and accurate summary, focusing only on the key points. "
            "Exclude all greeting words, and ensure the output does not contain blank lines or isolated '*'."
            "\n\n{COMMENTS}")
        
        msgComment, hisComment =  [], []
        for _, row in comment.iterrows():
            hisComment.append(row['content'].strip())
        
        msgComment.append(self.systemPrompt)
        msgComment.append({"role": "user", "content": userPrompt.format(COMMENTS = hisComment)})

        return msgComment

    def _processCombine(self, chat, email, comment):
        cols = ['createdAt', 'source', 'content', 'combineType']
        if not chat.empty:
            chat = chat.rename({'message':'content'},axis=1) 
            chat["combineType"] = "chat"
        else:
            chat = pd.DataFrame(columns=cols)
            
        if not email.empty:
            email = email.rename({'email':'content'},axis=1)
            email["combineType"] = "email"
        else:
            email = pd.DataFrame(columns=cols)
            
        if not comment.empty:
            comment["combineType"] = "comment"
        else:
            comment = pd.DataFrame(columns=cols)    
        
        noEmpty = [df for df in [chat[cols], email[cols], comment[cols]] if not df.empty]
        data = pd.concat(noEmpty, ignore_index=True).sort_values(by='createdAt', ignore_index=True)

        
        return data
             
    def _formatCombine(self, combine): 
        userPrompt = (
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
        
        msgCombine, hisCombine = [], []
        if not combine.empty:
            for _, row in combine.iterrows():
                hisCombine.append({
                    "role": row['combineType'] + " from " + row['source'] if pd.notna(row['source']) and (row['source'] in ["Clinic", "Insurance_Company", "DRP"]) else "Other",
                    "content": row['content'].strip()})
                
            combine_text = "\n".join([f"{item['role']}: {item['content']}" for item in hisCombine])
            
            msgCombine.append(self.systemPrompt)
            msgCombine.append({
                "role": "user",
                "content": userPrompt.format(COMBINE = combine_text)})
            
        return msgCombine
    
    def _fetchData(self, kind, condition):
        if kind == 'chat':
            query = self.summaryChatQuery.format(CONDITION=condition)
        elif kind == 'email':
            query = self.summaryEmailQuery.format(CONDITION=condition) 
        elif kind == 'comment':
            query = self.summaryCommentQuery.format(CONDITION=condition)
        else:
            return pd.DataFrame()
        
        df = fetchFromDB(query)
        if not df.empty:
            return df
        else:
            return pd.DataFrame()
        
    def _initialClient(self):
        api_key=os.getenv('GROQ_API_KEY')
        groqClient = Groq(api_key=api_key)
        return groqClient
   
    def _getAIResponse(self, groqClient, msg):
        summary, error_message = None, None
        try:
            chat_completion = groqClient.chat.completions.create(
                messages=msg,
                model=self.model
            )
            summary = chat_completion.choices[0].message.content
            parts = re.split(r':\s*', summary)
            if len(parts) > 1:
                summary = ":".join(parts[1:])
                
        except groq.APIConnectionError as e:
            error_message = "The server could not be reached. Please try again later."
        except groq.RateLimitError:
            error_message = "Rate limit exceeded. Please try again later."
        except groq.APIStatusError as e:
            error_message = f"API Error: {e.status_code}, Response: {e.response}"
            
        return summary, error_message
    
    def main(self, condition, useCase='api'):
        summaryClinic,summaryIC,summaryEmail,summaryCommentDR,summaryCommentEmail,summaryCombine = None, None, None, None, None, None
        errorChat, errorEmail, errorCommentDR, errorCommentEmail, errorCombine = None, None, None, None, None
        clinicName, icName, sender, recipient = None, None, None, None
    
        groqClient = self._initialClient()
        
      # chat    
        chat = self._fetchData('chat', condition['chat'])
        if not chat.empty:
            chat = self._processChat(chat)
            if useCase == 'webService':
                chatClinic = chat[(chat['type_'] == 'errand') & (chat['message'].notna())]
                chatIc = chat[(chat['type_'] == 'insurance_company_errand') & (chat['message'].notna())]
                
                if (not chatClinic.empty) and (chatClinic['reference'].notna().any()):
                    msgClinic, clinicName = self._formatChat(chatClinic)
                    if len(msgClinic)>0:
                        summaryClinic, errorChat = self._getAIResponse(groqClient, msgClinic) 
                        
                if (not chatIc.empty) and (chatIc['reference'].notna().any()): 
                    msgIC, icName = self._formatChat(chatIc)
                    if len(msgIC)>0:
                        summaryIC, errorChat = self._getAIResponse(groqClient, msgIC) 
        else:
            errorChat = 'Ingen Chatt Tillgänglig.'
        
      # email    
        email = self._fetchData('email', condition['email'])
        if (not email.empty) and (email['id'].notna().any()):
            email = self._processEmail(email)
            if useCase == 'webService':
                msgEmail, sender, recipient = self._formatEmail(email)
                if len(msgEmail)>0:
                    summaryEmail, errorEmail = self._getAIResponse(groqClient, msgEmail)  
        else:
            errorEmail = 'Ingen Email Tillgänglig.'
            
            
      # comments        
        comment = self._fetchData('comment', condition['comment'])  
        if not comment.empty:
            comment = self._processComment(comment)
            if useCase == 'webService':
                commentDR = comment[comment['type'] == 'Errand'] 
                commentEmail = comment[comment['type'] == 'Email']
                
                if not commentDR.empty:
                    msgCommentDR = self._formatComment(commentDR)
                    if len(msgCommentDR)>0:
                        summaryCommentDR, errorCommentDR = self._getAIResponse(groqClient, msgCommentDR) 
                else:
                    errorCommentDR = 'Inga Kommentarer Tillgängliga för DR.'  
                
                if not commentEmail.empty:  
                    msgCommentEmail = self._formatComment(commentEmail)         
                    if len(msgCommentEmail)>0:
                        summaryCommentEmail, errorCommentEmail = self._getAIResponse(groqClient, msgCommentEmail) 
                else:
                    errorCommentEmail = 'Inga Kommentarer Tillgängliga för Email.'
        else:
            errorCommentDR, errorCommentEmail = 'Inga Kommentarer Tillgängliga för DR.', 'Inga Kommentarer Tillgängliga för Email.'
    
      # combine
        combine = self._processCombine(chat, email, comment)
        if (not combine.empty) and (combine['content'].notna().any()):
            msgCombine = self._formatCombine(combine)
            if len(msgCombine)>0:
                summaryCombine, errorCombine = self._getAIResponse(groqClient, msgCombine)  
        else:
            errorCombine = "Inga tillgängliga data" 
        
        if useCase == 'webService':
            return (summaryClinic, clinicName, summaryIC, icName, errorChat, 
                    summaryEmail, sender, recipient, errorEmail, 
                    summaryCommentDR, errorCommentDR, summaryCommentEmail, errorCommentEmail, 
                    summaryCombine, errorCombine)
        elif useCase == 'api':
            return (summaryCombine, errorCombine)
    
        
        

