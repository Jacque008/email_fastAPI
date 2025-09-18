import pandas as pd
import re
from .llmSummary import LLMSummary
from .utils import fetchFromDB

# Chronological Log
    # URL: https://classify-emails-596633500987.europe-west4.run.app/log_api

    # Usage: The API retrieves all related info associated with the provided errand from creating to closing, 
    # and makes them into a chronological log. After feeding the log to an AI model (currently is llama-3.3-70b-versatile).
    # It will get the analysis and the risk assessment.
    
    # Retrieved Info includes the following nodes:
        #  'Errand_Created':
        #  'Send_To_IC':
        #  'Update_DR':
        #  'Email':
        #  'Chat': 
        #  'Comment':
        #  'Create_Invoice':
        #  'Receive_Payment_From_IC':
        #  'Receive_Payment_From_DÄ':
        #  'Pay_Out_To_CLinic':
        #  'Pay_Back_To_Customer':
        #  'Errand_Cancellation':
        #  'Errand_Cancellation_Reversed':

    # Input Data (Only one at a time):
        #  [{"Errand Number": "" }]

    # Output Data (will be also one at a time):
        #     [{          Ärenden: 58157
        #   ******************* Sammanfattning och Analys *******************
        # - Saknade steg: Inga
        # - Nästa steg: Inga
        # - Tidsproblem: Inga
        # - Betalningsavvikelser: Inga
        # - Undantag: Inga
        # - Särskilda riskvarningar: Inga
        # - Anledning till avbokning: Inga
        # - Risknivå: Låg / Medel / Hög
        # -----------------------------------------------------------------------------------------------------------------------
        # 2025-01-16
        # • At 18:38 Direktregleringsärendet skapades av klinik Nynäshamns Djurklinik.
        # • At 18:38 Direktregleringsärendet skickades till försäkringsbolag If.
        # 2025-01-20
        # • At 09:14 Nynäshamns Djurklinik skickade ett chattmeddelande:
        #            Godmorgon! Kan ni skicka om denna? mvh,Annika
        # • At 09:21 Erik skickade ett chattmeddelande:
        #            Hej! Jag kontaktar If!
        # • At 10:27 Försäkringsbolag skickade ett Specifik Information emejl med följande innehåll:
        #            [SUBJECT]SV: Saknar ersättningsbesked (Referens: 1000578904, Försäkringsnummer: KD193348-0798-01)
        #            [BODY]Skickat: 20 januari 2025 (content is removed from here)
        # • At 11:16 Försäkringsbolag skickade ett Ersättningsbesked emejl med följande innehåll:
        #            [SUBJECT]Ärende 247251337 hos If Skadeförsäkring Ab
        #            [BODY]Skickat: 20 januari 2025 (content is removed from here)
        # • At 11:34 Direktregleringsärendet uppdaterade ersättningsbeloppet 2108.00 kr manually.
        # • At 14:36 En swedbankPay faktura skapades för 521.00 kr.
        # 2025-01-24
        # • At 14:05 Mottog betalning på 521.00 kr från djurägare(Camilla Olsson).
        # 2025-01-29
        # • At 01:00 Mottog betalning på 2108.00 kr från försäkringsbolag(If).
        # 2025-01-30
        # • At 01:00 Betalade 2480.00 kr till klinik Nynäshamns Djurklinik.
        #         }]
 
class ChronologicalLog(LLMSummary):
    def __init__(self, cond1, cond2, groqClient): 
        super().__init__()
        self.cond1 = cond1
        self.cond2 = cond2
        self.groqClient = groqClient
        
        self.logBaseQuery = self.queries['logBase'].iloc[0] # keep for log and app.py
        self.logEmailQuery = self.queries['logEmail'].iloc[0] # keep for log and app.py
        self.logChatQuery = self.queries['logChat'].iloc[0] # keep for log and app.py
        self.logCommentQuery = self.queries['logComment'].iloc[0] # keep for log and app.py
        self.logInvoiceSPQuery = self.queries['logInvoiceSP'].iloc[0] # keep for log and app.py
        self.logInvoiceFOQuery = self.queries['logInvoiceFO'].iloc[0] # keep for log and app.py
        self.logInvoiceKAQuery = self.queries['logInvoiceKA'].iloc[0] # keep for log and app.py
        self.logReceiveQuery = self.queries['logReceive'].iloc[0] # keep for log and app.py
        self.logCancelQuery = self.queries['logCancel'].iloc[0] # keep for log and app.py
        self.logRemoveCancelQuery = self.queries['logRemoveCancel'].iloc[0] # keep for log and app.py
        
        self.columns = ['errandId', 'node','timestamp','itemId','msg','involved','source']
        self.colmapping = {'node':'Nod','timestamp':'Tid','msg':'Innehåll','involved':'Inblandade'}
        self.categoryMapping = {'Auto_Reply':'Auto-Svar','Finance_Report':'Ekonomirapport','Information':'Generell Information','Settlement_Request':'Förhandsbesked',
                                'Message':'Specifik Information','Question':'Fråga','Settlement_Approved':'Ersättningsbesked','Settlement_Denied':'Ersättning Nekad',
                                'Wisentic_Error':'Fel i Djurskador','Complement_Damage_Request_Insurance_Company':'Komplettering Skadeanmälan','Other':'Övrigt','Spam':'Spam',
                                'Complement_Damage_Request_Clinic':'Komplettering Skadeanmälan','Complement_DR_Insurance_Company':'Komplettering Direktreglering',
                                'Complement_DR_Clinic':'Komplettering Direktreglering','Insurance_Validation_Error':'Felaktig Försäkringsinfo','Manual_Handling_Required':'Manuell Hantering',}

        self.drpFee = 149
        self.basicSteps = [ "Errand Created (Required and Errand Started)",
                            "Errand Submitted to Insurance Company (Required)",
                            "Email Correspondence Between Insurance Company and Clinic (Optional)",
                            "Compensation Amount Updated (Required)",
                            "Chat Communication Between Insurance Company, Clinic, and DRP (Optional)",
                            "Comment Added (Optional)",
                            "Invoice Generated (Required)",
                            "Payment Received from Insurance Company (Required)",
                            "Payment Received from Clinic (Required)",
                            "Payment to Clinic (Required and Errand Closed)"]
        self.timeRules  = [ "Errand submission must occur after creation.",
                            "Invoicing must occur after compensation amount updates.",
                            "It is normal for email or chat conversations between the insurance company and the clinic to occur over multiple days.",
                            "Insurance company payments must occur after compensation amount updates, a delay of several days is normal.",
                            "Customer payments must occur after invoicing, a delay of several days is normal.",
                            "Errand can be cancelled at anytime."]
        self.paymentRules = [ "Compensation amount updates can be performed via the Agria API, auto-matching, directly in email, or manually.", 
                            "Payment from the insurance company must match the updated compensation amount.",
                            "Payment from the customer must match the invoice amount.",
                            "Invoices are issued only to customers, not to the insurance company.",
                            "Compensation does not need to be related to the invoice amount."]
        self.logicalRules = ["If an action is normal or follows the logical rules, do not mention it in the response.",
                            "If any action violates the logical rules, explicitly mention it in the response."]

    def _errandBase(self):
        base = fetchFromDB(self.logBaseQuery.format(COND1=self.cond1, COND2=self.cond2))
        float_cols = base.select_dtypes(include=['float64']).columns
        base[float_cols] = base[float_cols].astype('Int64')

        return base
    
    def _createErrand(self, base):
        create = base[['errandId','errandCreaTime','errandNumber','clinicName']]
        if create.empty:
            create = pd.DataFrame(columns=self.columns)
        else:
            create.loc[:,['node','msg','source']] = ['Errand_Created', '', '']
            create = create.rename(columns={"errandCreaTime":"timestamp","errandNumber":"itemId","clinicName":"involved"}).drop_duplicates()
            create['timestamp'] = pd.to_datetime(create['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            create['itemId'] = create['itemId'].apply(lambda row: 'errandNr: ' + row)
            create = create[self.columns]
        
        return create
    
    def _sendToIC(self, base):
        send = base.loc[base['sendTime'].notna(),['errandId','insuranceCaseId','reference','sendTime','insuranceCompanyName']]
        if send.empty:
            send = pd.DataFrame(columns=self.columns)
        else:
            send.loc[:,['node','source']] = ['Send_To_IC','']
            send = send.rename(columns={"insuranceCaseId":"itemId","sendTime":"timestamp","reference":"msg","insuranceCompanyName":"involved"}).drop_duplicates()
            send['timestamp'] = pd.to_datetime(send['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            send['itemId'] = send['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
            send['msg'] = send['msg'].apply(lambda row: 'reference: ' + str(row))
            send = send[self.columns]
        
        return send
    
    def _emailData(self):
        emailBase = fetchFromDB(self.logEmailQuery.format(COND1=self.cond1, COND2=self.cond2))
        
        emailBaseColumns = ['errandId', 'emailId', 'subject', 'textPlain', 'textHtml', 'emailTime', 'category', 'correctedCategory', 'source']

        if emailBase.empty:
            email = pd.DataFrame(columns=self.columns)
            emailBase = pd.DataFrame(columns=emailBaseColumns)
        else:
            emailBase = emailBase[emailBaseColumns]
            emailBase[['origin','email']] = emailBase.apply(lambda row: self._mergeText(row['subject'], row['textPlain'], row['textHtml']), axis=1).apply(pd.Series)
            email = emailBase.copy()
            email.loc[:,['node']] = 'Email'
            email['correctedCategory'] = email['correctedCategory'].fillna(email['category'])
            email['correctedCategory'] = email['correctedCategory'].map(self.categoryMapping)
            email = email.rename(columns={"emailId":"itemId","emailTime":"timestamp","email":"msg","correctedCategory":"involved"}).drop_duplicates()
            email['timestamp'] = pd.to_datetime(email['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            email['itemId'] = email['itemId'].apply(lambda row: 'emailId: ' + str(row))
            email = email[self.columns]
        
        return email, emailBase
    
    def _chatData(self, base):
        chat = fetchFromDB(self.logChatQuery.format(COND1=self.cond1, COND2=self.cond2))
        if chat.empty:
            chat = pd.DataFrame(columns=self.columns)
        else:
            chat = pd.merge(chat, base, on='errandId', how='inner')
            chat.loc[:, ['node','involved','source']] = ['Chat', '', '']
            for idx, row in chat.iterrows():
                if pd.notna(row['chatDRP']):
                    chat.at[idx, 'involved'] = f"{row['chatDRP']}"
                elif pd.notna(row['chatClinic']):
                    chat.at[idx, 'involved'] = f"{row['clinicName']}"
                elif pd.notna(row['chatMessageId']):
                    chat.at[idx, 'involved'] = f"{row['insuranceCompanyName']}"
            chat = chat.rename(columns={"chatMessageId":"itemId","chatTime":"timestamp","message":"msg"}).drop_duplicates()   
            chat['timestamp'] = pd.to_datetime(chat['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            chat['itemId'] = chat['itemId'].apply(lambda row: 'chatMessageId: ' + str(row))
            chat = chat[self.columns]
            
        return chat
    
    def _commentData(self, base):
        comment = fetchFromDB(self.logCommentQuery.format(COND1=self.cond1, COND2=self.cond2))
        if comment.empty:
            comment = pd.DataFrame(columns=self.columns)
        else:
            comment = pd.merge(comment, base, on='errandId', how='inner')
            comment =comment[['errandId','commentId','commentTime','commentDRP','content']]
            comment.loc[:,['node','source']] = ['Comment','']
            comment = comment.rename(columns={"commentId":"itemId","commentTime":"timestamp","commentDRP":"involved","content":"msg"}).drop_duplicates()
            comment['itemId'] = comment['itemId'].apply(lambda row: 'commentId: ' + str(row))
            comment['timestamp'] = pd.to_datetime(comment['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            comment = comment[self.columns]
        
        return comment
    
    def _updateErrand(self, base, emailBase):
        update = base.loc[(base['settlementAmount'].notna()) & (base['updatedTime'].notna()),['errandId','insuranceCaseId','updatedTime','settlementAmount']]

        if update.empty:
            update = pd.DataFrame(columns=self.columns)
        else:
            involved = pd.merge(update, emailBase[['errandId','category','correctedCategory']], on='errandId', how='left')            
            involved.loc[involved['category'].isna(),'involved'] = 'by Agria API'
            involved.loc[involved['category'].str.startswith('Settlement') & involved['correctedCategory'].isna(),'involved'] = 'by auto-matching'
            involved.loc[involved['correctedCategory'].notna() & involved['correctedCategory'].str.startswith('Settlement'),'involved'] = 'manually'
            involved = involved.drop_duplicates()
            noInvolved = involved['involved'].isna().all()
            if noInvolved:
                involved.loc[involved['settlementAmount'].notna() & involved['involved'].isna(), 'involved'] = 'directly in email'
            else:
                involved = involved.dropna(subset=['involved'])
                
            update.loc[:,['node','source']] = ['Update_DR','']
            update = pd.merge(update, involved[['errandId','involved']], on='errandId', how='left')
            
            update = update.rename(columns={"insuranceCaseId":"itemId","updatedTime":"timestamp","settlementAmount":"msg"}).drop_duplicates()
            update['timestamp'] = pd.to_datetime(update['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            update['msg'] = update['msg'].apply(lambda row: f"{row} kr")
            update['itemId'] = update['itemId'].apply(lambda row: 'insuranceCaseId: ' + str(row))
            update = update[self.columns]

        return update
    
    def _createInvoice(self):
        invoiceSwedbank = fetchFromDB(self.logInvoiceSPQuery.format(COND1=self.cond1, COND2=self.cond2))
        invoiceFortus = fetchFromDB(self.logInvoiceFOQuery.format(COND1=self.cond1, COND2=self.cond2))
        invoiceKassa = fetchFromDB(self.logInvoiceKAQuery.format(COND1=self.cond1, COND2=self.cond2)) 
        
        invoice = pd.DataFrame()
        for df in [invoiceSwedbank, invoiceFortus, invoiceKassa]:
            if not df.empty:
                invoice = pd.concat([invoice, df], ignore_index=True)

        if invoice.empty:
            invoice = pd.DataFrame(columns=self.columns)
        else:
            invoice = invoice.assign(node='Create_Invoice', source='')
            invoice = invoice.rename(columns={"invoiceNumber":"itemId","transTime":"timestamp","invoiceAmount":"msg","paymentOption":"involved"}).drop_duplicates()
            invoice['timestamp'] = pd.to_datetime(invoice['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            invoice['itemId'] = invoice['itemId'].apply(lambda row: 'invoiceNr: ' + str(row) if pd.notna(row) else "No Invoice")
            invoice['msg'] = invoice['msg'].apply(lambda row: f"{str(row).replace('-','')} kr" if pd.notna(row) else "No Invoice")
            invoice = invoice[self.columns]
        
        return invoice
    
    def _receivePayment(self):
      # payment from IC
        condIC = f" AND a.\"ownerType\"='insurance_company' AND a.type_='receivable' AND tl.type_='settlement_payment_line' AND es.\"settlementPaid\" IS TRUE"
        if self.cond1 is True:
            payIC = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1, COND2=self.cond2+condIC))
        elif self.cond2 is True:
            payIC = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1+condIC, COND2=self.cond2))
        payIC.loc[:,['node']] = 'Receive_Payment_From_IC'
        payIC['accountingDate'] = payIC['accountingDate'].fillna(payIC['createdAt'])
        
      # payment from DÄ
        condDA = " AND a.\"ownerType\"='animal_owner' AND a.type_='receivable' AND tl.type_='customer_payment_line' AND es.\"customerPaid\" IS TRUE"
        if self.cond1 is True:
            payDA = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1, COND2=self.cond2+condDA))
        elif self.cond2 is True:
            payDA = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1+condDA, COND2=self.cond2))
        payDA.loc[:,['node']] = 'Receive_Payment_From_DÄ'
        payDA['accountingDate'] = pd.to_datetime(payDA['accountingDate'], errors='coerce', utc=True)
        payDA['accountingDate'] = payDA['accountingDate'].fillna(payDA['createdAt'])

      # payout to Clinic
        condPay = " AND a.\"ownerType\"='clinic' AND a.type_='cash' AND tl.type_='veterinary_payout_line' AND es.\"disbursed\" IS TRUE"
        if self.cond1 is True:
            payout = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1, COND2=self.cond2+condPay))
        elif self.cond2 is True:
            payout = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1+condPay, COND2=self.cond2))
        payout.loc[:,['node']] = 'Pay_Out_To_CLinic'
        payout['accountingDate'] = payout['accountingDate'].fillna(payout['createdAt'])

      # pay back to Customer
        condBack = " AND a.\"ownerType\"='animal_owner' AND a.type_='receivable' AND tl.type_='customer_reversal_line'"
        if self.cond1 is True:
            payback = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1, COND2=self.cond2+condBack))
        elif self.cond2 is True:
            payback = fetchFromDB(self.logReceiveQuery.format(COND1=self.cond1+condBack, COND2=self.cond2))
        payback.loc[:,['node']] = 'Pay_Back_To_Customer'
        payback['accountingDate'] = payback['accountingDate'].fillna(payback['createdAt'])
        
      # concatenate
        payment = pd.DataFrame()
        for df in [payIC, payDA, payout, payback]:
            if not df.empty:
                payment = pd.concat([payment, df])
                
        if payment.empty:
            payment = pd.DataFrame(columns=self.columns)
        else:
            payment['source'] = payment['amount']
            payment = payment.rename(columns={"transactionId":"itemId","accountingDate":"timestamp","name":"involved","amount":"msg"}).drop_duplicates()
            payment['timestamp'] = pd.to_datetime(payment['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            payment['msg'] = payment['msg'].apply(lambda row: f"{str(row).replace('-','')} kr")
            payment['itemId'] = payment['itemId'].apply(lambda row: 'transactionId: ' + str(row))
            payment['involved'] = payment['involved'].replace(r'( klientmedel| kundreskontra)$', '', regex=True)
            payment = payment[self.columns]
        
        return payment
    
    def _cancelData(self):
        cancel = fetchFromDB(self.logCancelQuery.format(COND1=self.cond1, COND2=self.cond2))
        if cancel.empty:
            cancel = pd.DataFrame(columns=self.columns)
        else:
            cancel.loc[:,['node','msg', 'involved','source']] = ['Errand_Cancellation','Cancelled', '','']
            cancel = cancel.rename(columns={"transactionId":"itemId","cancelTime":"timestamp"}).drop_duplicates()
            cancel['timestamp'] = pd.to_datetime(cancel['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            cancel['itemId'] = cancel['itemId'].apply(lambda row: 'transactionId: ' + str(row))
            cancel = cancel[self.columns]
        
        return cancel

    def _removeCancel(self):
        remove = fetchFromDB(self.logRemoveCancelQuery.format(COND1=self.cond1, COND2=self.cond2))
        if remove.empty:
            remove = pd.DataFrame(columns=self.columns)
        else:
            remove.loc[:,['node','msg', 'involved','source']] = ['Errand_Cancellation_Reversed','Reinstated', '','']
            remove = remove.rename(columns={"transactionId":"itemId","removeTime":"timestamp"}).drop_duplicates()
            remove['timestamp'] = pd.to_datetime(remove['timestamp'], utc=True).dt.tz_convert('Europe/Stockholm')
            remove['itemId'] = remove['itemId'].apply(lambda row: 'transactionId: ' + str(row))
            remove = remove[self.columns]
        
        return remove
    
    def _riskAssessing(self, doc):
        system_prompt = f"""
            You are an expert in log analysis and risk assessment. Your task is to analyze errand logs in Swedish based on the following predefined steps and logical rules:

            ### Predefined Steps:
            {", ".join(self.basicSteps)}

            ### Time Rules:
            {", ".join(self.timeRules)}

            ### Payment Rules:
            {", ".join(self.paymentRules)}

            ### Logical Rules:
            {", ".join(self.logicalRules)}

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

            ### Output Format
            Your analysis should be structured as follows and output in Swedish:

                             ******************* Sammanfattning och Analys *********************
            - Saknade steg: [Lista saknade obligatoriska steg. Om inga, ange "Inga".]
            - Nästa steg: [Lista nästa steg som behöver utföras om "Payout to Clinic" inte finns. Om inga, ange "Inga".]
            - Tidsproblem: [Lista endast oväntade tidsrelaterade problem. Om inga, ange "Inga".]
            - Betalningsavvikelser: [Lista avvikelser om ' xxx kr' finns, med exakta belopp. Om inga, ange "Inga".]
            - Undantag: [Lista endast regelöverträdelser. Om inga, ange "Inga".]
            - Särskilda riskvarningar: [Lista eventuella 30-dagarsvarningar. Om inga, ange "Inga".]
            - Anledning till avbokning: [Om ärendet är avbrutet, ange anledningen. Annars, ange "Inga".]
            - Risknivå [Låg / Medel / Hög]
                • [Kort motivering 1 i punktform] (endast om risknivå är Medel eller Hög)
                • [Kort motivering 2 i punktform] (endast om risknivå är Medel eller Hög)
            """
        user_prompt = f"Analyze the following errand log and response in Swedish: {doc}"

        chat_completion = self.groqClient.chat.completions.create(
            messages=[{"role": "system","content": system_prompt},
                      {"role": "user","content": user_prompt}],
            model=self.model,)
        output = chat_completion.choices[0].message.content

        return output
       
    def _createLog(self,base,create,send,email,chat,comment,update,invoice,payment,cancel,remove):
        def filter_columns(df):
            return df.loc[:, df.notna().any()]

        processed_dfs = [
            filter_columns(df) 
            for df in [create, send, email, chat, comment, 
                    update, invoice, payment, cancel, remove]]

        log = pd.concat(processed_dfs, ignore_index=True).drop_duplicates()
        log = pd.merge(log, base[['errandId','clinicName','insuranceCompanyName']], on='errandId')

        float_cols = log.select_dtypes(include=['float64']).columns
        log[float_cols] = log[float_cols].astype('Int64')
        log = log[['errandId', 'node','timestamp','itemId','msg','involved','source','clinicName','insuranceCompanyName']].sort_values('timestamp')
        
        grouped = log.groupby('errandId')
        groupLog, groupAI = {}, {}

        doc = []
        for group_id, group_df in grouped:
            group_df = group_df.sort_values('timestamp').reset_index(drop=True)
            group_df['timestamp'] = group_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
            paragraph = f"Ärenden: {group_id}\n\n"
            discrepancy = 0
            date = {}
            placeholder = '€' * 11
            for idx, (_, row) in enumerate(group_df.iterrows()):
                date[idx] = row['timestamp'][:10]
                if idx > 0 and (idx - 1) in date and date[idx] == date[idx - 1]:
                    showDate = f'• At {row["timestamp"][11:]}'
                else:
                    showDate = '(COLORBLUE)' + date[idx] + '(/SPAN)\n• At '+row["timestamp"][11:]
                if row['node']=='Errand_Created':
                    paragraph += f"{showDate} (BOLD)Direktregleringsärendet skapades av klinik {row['involved']}.(/BOLD)\n"
                elif row['node']=='Send_To_IC':
                    paragraph += f"{showDate} (BOLD)Direktregleringsärendet skickades till försäkringsbolag {row['involved']}.(/BOLD)\n"
                elif row['node']=='Update_DR':
                    paragraph += f"{showDate} (BOLD)Direktregleringsärendet uppdaterade ersättningsbeloppet {row['msg']} {row['involved']}.(/BOLD)\n"
                elif row['node']=='Email':
                    row['msg'] = re.sub(r'\n+', f'\n{placeholder}', row['msg'])
                    row['msg'] = re.sub(r'\n+$', '', row['msg'])  
                    if (row['source']=='Clinic'):
                        paragraph += f"{showDate} (BOLD)Klinik skickade ett {row['involved']} emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){row['msg']}(/SPAN)(/ITALIC)\n"
                    elif (row['source']=='Insurance_Company'):
                        paragraph += f"{showDate} (BOLD)Försäkringsbolag skickade ett {row['involved']} emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){row['msg']}(/SPAN)(/ITALIC)\n"
                    else:
                        paragraph += f"{showDate} (BOLD){row['source']} skickade ett {row['involved']} emejl med följande innehåll:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){row['msg']}(/SPAN)(/ITALIC)\n"
                elif row['node']=='Chat':
                    row['msg'] = re.sub(r'\n+', f'\n{placeholder}', row['msg'])
                    row['msg'] = re.sub(r'\n+$', '', row['msg'])  
                    paragraph += f"{showDate} (BOLD){row['involved']} skickade ett chattmeddelande:(/BOLD)\n{placeholder}(ITALIC)(COLORGRAY){row['msg']}(/SPAN)(/ITALIC)\n"
                elif row['node']=='Comment':
                    row['msg'] = re.sub(r'\n+', f'\n{placeholder}', row['msg'])
                    row['msg'] = re.sub(r'\n+$', '', row['msg'])  
                    paragraph += f"{showDate} (BOLD){row['involved']} lämnade en kommentar:(/BOLD) \n{placeholder}(ITALIC)(COLORGRAY){row['msg']}(/SPAN)(/ITALIC)\n"
                elif row['node']=='Create_Invoice':
                    paragraph += f"{showDate} (BOLD)En {row['involved']} faktura skapades för {row['msg']}.(/BOLD)\n"
                elif row['node']=='Receive_Payment_From_IC': 
                    paragraph += f"{showDate} (BOLD)Mottog betalning på {row['msg']} från försäkringsbolag({row['involved']}).(/BOLD)\n"
                    discrepancy += abs(row['source'])
                elif row['node']=='Receive_Payment_From_DÄ':
                    paragraph += f"{showDate} (BOLD)Mottog betalning på {row['msg']} från djurägare({row['involved']}).(/BOLD)\n"
                    discrepancy += abs(row['source'])
                elif row['node']=='Pay_Out_To_CLinic':
                    paragraph += f"{showDate} (BOLD)Betalade {row['msg']} till klinik {row['involved']}.(/BOLD)\n"
                    discrepancy -= abs(row['source'])
                elif row['node'] == 'Pay_Back_To_Customer':
                    paragraph += f"{showDate} (BOLD)Återbetalade {row['msg']} till djurägare {row['involved']}.(/BOLD)\n"
                    discrepancy -= abs(row['source'])
                elif row['node']=='Errand_Cancellation':
                    paragraph += f"{showDate} (BOLD)Direktregleringsärendet avslutades.(/BOLD)\n" 
                elif row['node']=='Errand_Cancellation_Reversed':
                    paragraph += f"{showDate} (BOLD)Direktregleringsärendet återaktiverades.(/BOLD)\n" 
                    
            if (discrepancy == self.drpFee) or (discrepancy == 0):
                paragraph = f"Ärenden: {group_id} °Betalningsavvikelse: Nej§\n\n" + paragraph[len(f"Ärenden: {group_id}\n\n"):]
            else:
                paragraph = f"Ärenden: {group_id} °Betalningsavvikelse: {int(discrepancy)}kr§\n\n" + paragraph[len(f"Ärenden: {group_id}\n\n"):]
                
            doc.append(paragraph)
            doc_text = "\n".join(doc)
            log_text = (doc_text.replace('(COLORBLUE)', '<span style="color:blue;">')
                                          .replace('(COLORGRAY)', '<span style="color:gray;">')  # Fix COLORGRAY handling
                                          .replace('(ITALIC)', '<i>')
                                          .replace('(/ITALIC)', '</i>')
                                          .replace('(/SPAN)', '</span>')
                                          .replace('(BOLD)', '<b>')
                                          .replace('(/BOLD)', '</b>'))
            log_text = re.sub(r'[\t\r\n]+', '<br>', log_text)
            log_text = re.sub(r'(<br>€€€€€€€€€€€)+', '<br>€€€€€€€€€€€', log_text)
            formatted_log = (log_text.replace('<br>€€€€€€€€€€€</span></i><br>','</span></i><br>')
                                         .replace('€', '&nbsp;')
                                         .replace('<p><br></p>', ''))
            print("formatted_log: \n", formatted_log)
            groupLog[group_id] = formatted_log
            doc_text = (doc_text.replace('€','')
                                .replace('(COLORBLUE)', '')
                                .replace('(COLORGRAY)', '')  # Fix COLORGRAY handling
                                .replace('(ITALIC)', '')
                                .replace('(/ITALIC)', '')
                                .replace('(/SPAN)', '')
                                .replace('(BOLD)', '')
                                .replace('(/BOLD)', ''))
            ai_text = self._riskAssessing(doc_text)+'\n'
            ai_text = re.sub(r'(- Risknivå: .*?)\n+((?:\s*.*\n?)+)',  lambda m: f'<span style="color:red;">{m.group(1)}</span><br>'
                            f'<ul style="margin-left: 5px;">'+''.join(f'{line.strip()}\n' for line in m.group(2).split("\n") if line.strip()) +'</ul>', ai_text)
            ai_text = re.sub(r'[\t\r\n]+', '<br>', ai_text) 
            groupAI[group_id] = (ai_text.replace('******************* Sammanfattning och Analys *********************',
                                                '<div style="text-align: center;">******************* Sammanfattning och Analys *********************</div>')
                                       .replace('<p><br></p>', ''))
              
        groupLog = { group_id: {"title": text.split('°', 1)[0],"content": text.split('§', 1)[1]} for group_id, text in groupLog.items()}

        return groupLog, groupAI
    
    def main(self):
        base = self._errandBase()
        create = self._createErrand(base)
        send = self._sendToIC(base)
        email, emailBase = self._emailData()
        chat = self._chatData(base)
        comment = self._commentData(base)
        update = self._updateErrand(base, emailBase)
        invoice = self._createInvoice()
        payment = self._receivePayment()
        cancel = self._cancelData()
        remove = self._removeCancel()
        groupLog, groupAI = self._createLog(base,create,send,email,chat,comment,update,invoice,payment,cancel,remove)
        
        return groupLog, groupAI
    
