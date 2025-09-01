import regex as reg
import pandas as pd
from itertools import combinations
from .preprocess import PreProcess
from .utils import get_payoutEntity, fetchFromDB

# Inbetalning-Matching API
# URL: https://classify-emails-wisentic-596633500987.europe-west4.run.app/payment_api
# Input Data (multipul rows at a time):
    # [{
    #     "id" : 44353,
    #     "amount" : 113700,
    #     "reference" : "1000522704",
    #     "info" : "SKADEUTBETALNING\n\nSKADENUMMER: 7160833-1\nFAKTURANUMMER: 11240013235\nSPECIFIKATION ENLIGT DIREKTREGLERING\n",
    #     "bankName" : "Agria Djurförsäkring",
    #     "createdAt" : "2024-11-05 13:49:18.137 +0100"
    # }]
# Output Data (multipul rows at a time):
    # [{
    #     "id": 44354,
    #     ---Payment info:------
    #     "reference": ,
    #     "bankName": ,
    #     "amount": ,
    #     "info": ,
    #     "createdAt":  ,
    #     ---Matched info:------
    #     "insuranceCaseId": 52517,
    #     "status": "",
    # }]
    
class PaymentMatching(PreProcess):
    def __init__(self):
        super().__init__()
        self.infoReg = pd.read_csv(f"{self.folder}/infoReg.csv")
        self.infoItemList = self.infoReg.item.to_list()
        self.bankMap = pd.read_csv(f"{self.folder}/bankMap.csv")
        self.bankDict = self.bankMap.set_index('bankName')['insuranceCompanyReference'].to_dict()
        self.payoutEntity = get_payoutEntity()
        self.matchingColsPay = ['extractReference','extractOtherNumber','extractDamageNumber']
        self.matchingColsErrand = ['isReference','damageNumber','invoiceReference','ocrNumber']
        self.baseUrl = 'https://admin.direktregleringsportalen.se/errands/'         
        self.paymentQuery = self.queries['payment'].iloc[0] # keep for paymentMatching and app.py
        self.errandPayQuery = self.queries['errandPay'].iloc[0] # keep for paymentMatching and app.py
        self.errandLinkQuery = self.queries['errandLink'].iloc[0] # keep for paymentMatching and app.py 
        self.payoutQuery = self.queries['payout'].iloc[0] # keep for paymentMatching and app.py
        
    def _processPayment(self, pay):
        pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm') 

        refReg = reg.compile(r'\d+')
        pay.loc[pay['reference'].notna(),'extractReference'] = pay.loc[pay['reference'].notna(),'reference'].apply(lambda x: ''.join(refReg.findall(x)) if isinstance(x, str) else None)
        pay.loc[pay['extractReference'].notna(),'extractReference'] = pay.loc[pay['extractReference'].notna(),'extractReference'].replace('', None)
        pay['settlementAmount'] = 0
        pay['status'] = ""

        for col in self.infoItemList:
            colName = col.split('_')[1]
            if colName not in pay.columns:
                pay[colName] = None
        
        init_columns = ['valPay', 'valErrand', 'isReference', 'insuranceCaseId', 'referenceLink'] #'colPay','colErrand'
        for col in init_columns:
            pay[col] = [[] for _ in range(len(pay))]
        
        return pay[['id','valPay','valErrand','amount','settlementAmount','isReference','insuranceCaseId','referenceLink',
                    'status','extractReference','extractDamageNumber','extractOtherNumber','bankName','info','reference','createdAt']]
        
    def _parseInfo(self, pay):
        mask = pay['info'].notna()
        for idxPay, rowPay in pay[mask].iterrows():
            matched_value = None
            ic = self.bankDict.get(rowPay['bankName'], 'None') 
            mask_info = self.infoReg['item'].str.startswith(ic)
            for _, rowInfoReg in self.infoReg[mask_info].iterrows():
                col = rowInfoReg['item'].split('_')[1]  
                pattern = rowInfoReg['regex']       
                compiled_pattern = reg.compile(pattern, reg.DOTALL | reg.IGNORECASE) 
                match = compiled_pattern.search(rowPay['info'])
                if match:
                    matched_value = match.group(1).strip()
                    if pd.isna(rowPay[col]):
                        pay.at[idxPay, col]= matched_value
                    else:
                        pay.at[idxPay, 'isReference'].append(matched_value)
                                
        pay.loc[pay['extractDamageNumber'] == pay['extractOtherNumber'], 'extractDamageNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractOtherNumber'], 'extractOtherNumber'] = None
        pay.loc[pay['extractReference'] == pay['extractDamageNumber'], 'extractDamageNumber'] = None
        
        return pay
        
    def _findMatches(self, pay, errand, idxPay, rowPay):
        matched = {colPay: [] for colPay in self.matchingColsPay}
        for colPay in self.matchingColsPay:
            valPay = rowPay[colPay]
            if pd.notna(valPay):
                valAmount = rowPay['amount']
                mask_errandBeforePayment = errand['createdAt'] <= rowPay['createdAt']

                for _, rowErrand in errand[mask_errandBeforePayment].iterrows():
                    for colErrand in self.matchingColsErrand:
                        valErrand = rowErrand[colErrand]
                        if pd.notna(valErrand) and (valPay == valErrand): 
                            if rowErrand['isReference'] not in pay.at[idxPay, 'isReference']:
                                pay.at[idxPay, 'isReference'].append(rowErrand['isReference']) 
                                pay.at[idxPay, 'valPay'].append(valPay)                          
                            if (valAmount == rowErrand['settlementAmount']) and (int(rowErrand['insuranceCaseId']) not in matched[colPay]):
                                matched[colPay].append(int(rowErrand['insuranceCaseId']))
                                pay.at[idxPay, 'valErrand'].append(valErrand)
                            
        matchedLists = [set(matched[colPay]) for colPay in self.matchingColsPay if matched[colPay]]
        if matchedLists: # keep order
            matchedInsuranceCaseID = [id for id in matchedLists[0] if all(id in col for col in matchedLists[1:])] # get the common insuranceCaseIds
            if not matchedInsuranceCaseID:  # if no common then union
                matchedInsuranceCaseID = []
                for col in matchedLists:
                    for id in col:
                        if id not in matchedInsuranceCaseID:
                            matchedInsuranceCaseID.append(id)
        else:
            matchedInsuranceCaseID = []  
        
        return matchedInsuranceCaseID

    def _generateLinks(self, colList1, colList2, condition):  
        links = []
        if len(colList1)>0:
            for id, valErrand in zip(colList1, colList2):
                if condition == 'ic.reference':
                    conditon = f"{condition} = '{id}'"
                elif condition == 'ic.id':
                    conditon = f"{condition} = {id}"
                result = fetchFromDB(self.errandLinkQuery.format(CONDITION=conditon))
                if not result.empty: # fetch one line per time so can use .iloc[0]
                    errandNumber = result.iloc[0]['errandNumber']
                    ref = result.iloc[0]['reference']
                    link = f'<a href="{self.baseUrl}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by {valErrand}">{ref}</a>'
                    links.append(link)
                else:
                    links.append(f'{ref} (No Corresponding Link)')
        return links

    def _matchByInfo(self, pay, errand): 
        matchedInsuranceCaseID, links = [], []
        
        mask = (pay['info'].notna() | pay['extractReference'].notna())
        for idxPay, rowPay in pay[mask].iterrows():
            matchedInsuranceCaseID = self._findMatches(pay, errand, idxPay, rowPay) 
            qty = len(matchedInsuranceCaseID)
            if qty>0:
                pay.at[idxPay, 'insuranceCaseId'].extend(matchedInsuranceCaseID) 
                links = self._generateLinks(rowPay['insuranceCaseId'], rowPay['valErrand'], 'ic.id')
                pay.at[idxPay, 'referenceLink'] = links
                if qty ==1:
                    pay.at[idxPay, 'status'] = f"One DR matched perfectly (reference: {', '.join(links)})."
                elif qty > 1:
                    pay.at[idxPay, 'status'] = f"Found {qty} matching DRs (references: {', '.join(links)}) and the payment amount matches each one."
            else:      
                pay.at[idxPay, 'status'] = "No Found" 

        return pay

    def _partlyAmountMatching(self, refAmountDict, target_amount):
        references = list(refAmountDict.keys())
        amounts = list(refAmountDict.values())
        
        for r in range(1, len(amounts) + 1):
            for combo in combinations(zip(references, amounts), r):
                combo_references, combo_amounts = zip(*combo)
                if sum(combo_amounts) == target_amount:
                    return list(combo_references)
                
        return None
 
    def _reminderUnmatchedAmount(self, pay): # for those mismatched payment and match them in whole errands        
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            matchedInsuranceCaseID, isReference, links = [], [], []
            refAmountDict = {}
            msg = "No Found"
            
            if (len(rowPay['isReference']) > 0) and (rowPay['isReference'] not in isReference):
                isReference.extend(rowPay['isReference'])
            if pd.notna(rowPay['extractReference']) and (len(rowPay['extractReference']) == 10) and (rowPay['extractReference'] not in isReference):
                isReference.append(str(rowPay['extractReference']))                 
            if pd.notna(rowPay['extractOtherNumber']) and (len(rowPay['extractOtherNumber']) == 10) and (rowPay['extractOtherNumber'] not in isReference):
                isReference.append(str(rowPay['extractOtherNumber']))
            if pd.notna(rowPay['extractDamageNumber']) and (len(rowPay['extractDamageNumber']) == 10) and (rowPay['extractDamageNumber'] not in isReference):
                isReference.append(str(rowPay['extractDamageNumber']))
                
            if len(isReference) > 0:
                conditon = f"""AND ic.reference IN ({', '.join([f"'{ref}'" for ref in set(isReference)])})"""  
                subErrand = fetchFromDB(self.errandPayQuery.format(CONDITION=conditon))
                subErrand.loc[subErrand['settlementAmount'].isna(), 'settlementAmount'] = 0
                if not subErrand.empty:
                    for _, rowSub in subErrand.iterrows():
                        if rowSub['insuranceCaseId'] not in matchedInsuranceCaseID:
                            pay.loc[idx, 'settlementAmount'] += rowSub['settlementAmount']  
                            matchedInsuranceCaseID.append(rowSub['insuranceCaseId'])
                            
                        if rowSub['isReference'] not in pay.at[idx, 'isReference']:
                            pay.at[idx, 'isReference'].append(rowSub['isReference'])
                            pay.at[idx, 'valPay'].append(rowSub['isReference']) # to make sure the count of row['isReference'] is the same as row['valPay']
                            
                        if rowSub['isReference'] not in refAmountDict:
                            refAmountDict[rowSub['isReference']] = rowSub['settlementAmount']
                        # else:
                        #     refAmountDict[rowSub['isReference']] += rowSub['settlementAmount']

            refList = pay.at[idx, 'isReference']
            valErrandList = pay.at[idx, 'valPay']  # !!! row['valPay']==row['valErrand']
            links = self._generateLinks(refList, valErrandList, 'ic.reference')
            pay.at[idx, 'referenceLink'] = links
            
            qty = len(matchedInsuranceCaseID)
            if qty > 0:
                totalSettlementAmount = pay.at[idx, 'settlementAmount']
                rowPaymentAmount = rowPay['amount']

                if rowPaymentAmount == totalSettlementAmount:
                    if qty == 1:
                        msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                    else:       
                        msg = f"Found {qty} matching DRs (references: {', '.join(links)}), and the total amount matches the payment."
                else:
                    matched_references = self._partlyAmountMatching(refAmountDict, rowPaymentAmount)
                    if matched_references:
                        matched_links = [link for link in links if any(ref in link for ref in matched_references)]
                        if len(matched_references) ==1:
                            msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                        else:
                            msg = f"Found {len(matched_references)} matching DRs (references: {', '.join(matched_links)}), and the total amount matches the payment."
                    else:
                        if qty == 1:
                            msg = f"Found 1 relevant DR (reference: {', '.join(links)}), but the amount does not match."
                        else:
                            msg = f"Found {qty} relevant DRs (references: {', '.join(links)}), but the amounts do not match."

            pay.at[idx, 'status'] = msg

        return pay   

    def _matchEntityandAmount(self, pay, errand):
        def msgForOne(oneLineDF):
            errandNumber = oneLineDF.iloc[0]['errandNumber']
            ref = oneLineDF.iloc[0]['isReference']
            if source == 'Insurance_Company':
                entity = oneLineDF.iloc[0]['insuranceCompanyName'] 
            elif source == 'Clinic':
                entity = oneLineDF.iloc[0]['clinicName']
                
            link = f'<a href="{self.baseUrl}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
            if amount == oneLineDF.iloc[0]['settlementAmount']:
                msg = f"One DR matched perfectly (reference: {link}) by both entity and amount."
            else:
                msg = "No Found"
                
            return msg
                
        payoutEntitySource = self.payoutEntity.set_index('payoutEntity')['source'].to_dict()
        icDict = self.payoutEntity.loc[self.payoutEntity['source'] == 'Insurance_Company'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
        clinicDict = self.payoutEntity.loc[self.payoutEntity['source'] == 'Clinic'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
        
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            ref, errandNumber, entity, msg, link = None, None, None, None, None
            icList, clinicList = [], []
            amount = rowPay['amount']
            
            source = payoutEntitySource.get(rowPay['bankName'], "Unknown")
            if source == 'Insurance_Company':
                icList = icDict.get(rowPay['bankName'], None) 
            elif source == 'Clinic':
                clinicList = clinicDict.get(rowPay['bankName'], None) 
            else:
                continue
            
            entityMatched = errand.loc[(errand['createdAt'] <= rowPay['createdAt']) & ((source=='Insurance_Company') & (errand['insuranceCompanyName'].isin(icList))) | ((source=='Clinic') & (errand['clinicName'].isin(clinicList)))]
            qty = entityMatched.shape[0]
            if  qty == 1:
                msg = msgForOne(entityMatched)
                    
            elif qty > 1:
                sameClinicAnmialGroup = entityMatched.groupby(['insuranceCompanyName','clinicName','animalId']) 
                for _, group_df in sameClinicAnmialGroup:
                    if group_df.shape[0] == 1:
                        msg = msgForOne(group_df)
                        
                    else:
                        refAmountDict, link, links, msg  = {}, None, [], None
                        for _, row in group_df.iterrows():
                            refAmountDict[row['isReference']] = row['settlementAmount']
                            errandNumber = row['errandNumber']
                            ref = row['isReference']
                            if source == 'Insurance_Company':
                                entity = row['insuranceCompanyName'] 
                            elif source == 'Clinic':
                                entity = row['clinicName']
                            else:
                                continue
                            
                            link = f'<a href="{self.baseUrl}{errandNumber}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
                            links.append((ref, link))
                        
                        matched_references = self._partlyAmountMatching(refAmountDict, amount)
                        if matched_references:
                            matched_links = [link for ref, link in links if ref in matched_references]
                            if len(matched_references) == 1:
                                msg = f"One DR matched perfectly (reference: {', '.join(matched_links)}) by both entity and amount."
                            else:
                                msg = f"Found {len(matched_references)} matching DRs (references: {', '.join(matched_links)}) by entity, and the total amount matches the payment."
                        else:
                            msg = "No Found"
            else:
                msg = "No Found"
                
            pay.at[idx, 'status'] = msg

        return pay
        
    def _matchPayout(self, pay, payout):
        mask = (pay['status'].isin(["No Found",""]))
        for idx, rowPay in pay[mask].iterrows():
            matchedTransId, matchedClinicName, matchedType= [],[],[]
            for col in self.matchingColsPay:
                valPay = rowPay[col]
                if pd.notna(valPay):
                    for _, rowPayout in payout.iterrows():
                        if (valPay == rowPayout['reference']) and (rowPay['amount']==rowPayout['amount']):
                            if pd.notna(rowPayout['transactionId']) and (rowPayout['transactionId'] not in matchedTransId):
                                matchedTransId.append(int(rowPayout['transactionId']))   
                            if pd.notna(rowPayout['clinicName']) and (rowPayout['clinicName'] not in matchedClinicName):
                                matchedClinicName.append(rowPayout['clinicName'])
                            if pd.notna(rowPayout['transactionId']) and (rowPayout['transactionId'] not in matchedType):
                                matchedType.append(rowPayout['type'])  

            qty = len(matchedTransId)
            if qty == 1:
                pay.at[idx, 'status'] = f"Payment has been paid out<br>             TransactionId: {matchedTransId[0]}<br>             Amount: {rowPay['amount'] / 100:.2f} kr<br>             Clinic Name: {matchedClinicName[0]}<br>             Type: {matchedType[0]}" 
            elif qty > 0:
                pay.at[idx, 'status'] = f"Payment has been paid out {qty} times<br>    TransactionId:{' '.join(map(str, matchedTransId))}<br>             Amount: {rowPay['amount'] / 100:.2f} kr<br>    Clinic Name: {' '.join(map(str, matchedClinicName))}<br>    Type: {' '.join(map(str, matchedType))}"
            elif qty == 0:
                pay.at[idx, 'status'] = 'No matching DRs found.'   

        return pay

    def _statistic(self, pay):
        all = pay.id.count()
        matched = pay[(~pay['status'].str.contains('No Found', na=False)) & (~pay['status'].str.contains('No matching DRs found', na=False))]
        perfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False) & (pay['status'].str.contains('§firstOnePerfect§', na=False))]
        mulPerfect = pay[pay['status'].str.contains('each one', na=False)]
        payout = pay[pay['status'].str.contains('paid out', na=False)]
        oneTotalAmountMatch = pay[pay['status'].str.contains('One DR matched perfectly', na=False) & pay['status'].str.contains('§oneTotalAmountMatch§', na=False)]
        allTotalAmountMatch = pay[pay['status'].str.contains('total amount matches the payment', na=False) & pay['status'].str.contains('§allTotalAmountMatch§', na=False)]
        partialOnePerfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False) & pay['status'].str.contains('§partialOneperfect§', na=False)]
        partialMultiTotalMatched = pay[pay['status'].str.contains('total amount matches the payment', na=False) & pay['status'].str.contains('§partialMultiTotalMatched§', na=False)]
        oneRelevant = pay[pay['status'].str.contains('amount does not match', na=False)]
        mulRelevant = pay[pay['status'].str.contains('amounts do not match', na=False)]
        entityOnePerfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False) & pay['status'].str.contains('entityOnePerfect', na=False)]
        entityPartialOnePerfect = pay[pay['status'].str.contains('One DR matched perfectly', na=False) & pay['status'].str.contains('entityPartialOnePerfect', na=False)]
        entityParialMulitMatched = pay[pay['status'].str.contains('by entity, and the total amount matches the payment', na=False) & pay['status'].str.contains('entityParialMulitMatched', na=False)]
        noFound = pay[pay['status'].str.contains('No Found', na=False)]
        noMatch = pay[pay['status'].str.contains('No matching DRs found', na=False)]
        
        # matched.to_csv(f"data/test_data/matched.csv",index=False)
        # unmatched.to_csv(f"data/test_data/unmatched.csv",index=False)
        print(f"""
        all the payment: {all}
        matched: {matched.id.count()}, rate:{matched.id.count()/all*100:.2f}%
            One Perfect Matched: {perfect.id.count()}, rate:{perfect.id.count()/all*100:.2f}%
            Multiple Each Amount Matched: {mulPerfect.id.count()}, rate:{mulPerfect.id.count()/all*100:.2f}%
            One Total Amount Matched: {oneTotalAmountMatch.id.count()}, rate:{oneTotalAmountMatch.id.count()/all*100:.2f}%
            All Total Amount Matched: {allTotalAmountMatch.id.count()}, rate:{allTotalAmountMatch.id.count()/all*100:.2f}%
            Partial One Total Amount Matched: {partialOnePerfect.id.count()}, rate:{partialOnePerfect.id.count()/all*100:.2f}%
            Partial Multi Total Amount Matched: {partialMultiTotalMatched.id.count()}, rate:{partialMultiTotalMatched.id.count()/all*100:.2f}%
            One Relevant But Amount Not Match: {oneRelevant.id.count()}, rate:{oneRelevant.id.count()/all*100:.2f}%
            Multiple Relevant But Amount Not Match: {mulRelevant.id.count()}, rate:{mulRelevant.id.count()/all*100:.2f}%
            Entity One Perfect Matched: {entityOnePerfect.id.count()}, rate:{entityOnePerfect.id.count()/all*100:.2f}% 
            Entity Partial One Perfect Matched: {entityPartialOnePerfect.id.count()}, rate:{entityPartialOnePerfect.id.count()/all*100:.2f}% 
            Entity Partial Multi Perfect Matched: {entityParialMulitMatched.id.count()}, rate:{entityParialMulitMatched.id.count()/all*100:.2f}%
        
        Paid Out: {payout.id.count()}, rate:{payout.id.count()/all*100:.2f}%
        
        unmatched: {noFound.id.count()+noMatch.id.count()}, rate:{(noFound.id.count()+noMatch.id.count())/all*100:.2f}%         
            No Found: {noFound.id.count()}, rate:{noFound.id.count()/all*100:.2f}%
            No matching DRs found: {noMatch.id.count()}, rate:{noMatch.id.count()/all*100:.2f}%""")  
        
        return None
     
    def main(self, payDf):
        pay = self._processPayment(payDf)
        pay = self._parseInfo(pay)
        errand = fetchFromDB(self.errandPayQuery.format(CONDITION=""))
        errand['createdAt'] = pd.to_datetime(errand['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')

        pay = self._matchByInfo(pay, errand)
        mask = (pay['status'].isin(["No Found",""]))
        if mask.any():
            pay = self._reminderUnmatchedAmount(pay)
            # display("_reminderUnmatchedAmount\n",pay)
        
        mask = (pay['status'].isin(["No Found",""]))
        if mask.any():
            pay = self._matchEntityandAmount(pay, errand)
            # display("_matchEntityandAmount\n",pay)
        
        mask = (pay['status'].isin(["No Found",""]))
        if mask.any():
            payout = fetchFromDB(self.payoutQuery)
            pay = self._matchPayout(pay, payout)
            # display("_matchPayout\n",pay)
            
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        
        # pay['status'] = pay['status'].str.replace(" ", "&nbsp;", regex=False)
        # self._statistic(pay)

        return pay[['id','bankName','amount','info','reference','createdAt','insuranceCaseId','status']]