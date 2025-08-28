"""
Payment Matching Workflow
支付匹配工作流
基于 old_flask_code/paymentMatching.py 重构
"""
import regex as reg
import pandas as pd
from itertools import combinations
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime

from ..app.services.utils import fetchFromDB, get_payout_entity
from ..app.services.base_service import BaseService


class PaymentMatchingWorkflow(BaseService):
    """支付匹配工作流类"""
    
    def __init__(self):
        super().__init__()
        self._setup_matching_configurations()
        self._setup_matching_queries()
        self._setup_matching_rules()
    
    def _setup_matching_configurations(self):
        """设置匹配配置"""
        try:
            self.info_reg = pd.read_csv(f"{self.folder}/infoReg.csv")
            self.info_item_list = self.info_reg.item.to_list()
            self.bank_map = pd.read_csv(f"{self.folder}/bankMap.csv")
            self.bank_dict = self.bank_map.set_index('bankName')['insuranceCompanyReference'].to_dict()
            self.payout_entity = get_payout_entity()
        except Exception as e:
            print(f"设置匹配配置失败: {str(e)}")
            # 设置默认值
            self.info_reg = pd.DataFrame()
            self.info_item_list = []
            self.bank_map = pd.DataFrame()
            self.bank_dict = {}
            self.payout_entity = pd.DataFrame()
    
    def _setup_matching_queries(self):
        """设置匹配查询语句"""
        try:
            self.payment_query = self.queries['payment'].iloc[0]
            self.errand_pay_query = self.queries['errandPay'].iloc[0]
            self.errand_link_query = self.queries['errandLink'].iloc[0]
            self.payout_query = self.queries['payout'].iloc[0]
        except Exception as e:
            print(f"设置匹配查询失败: {str(e)}")
            # 设置默认空查询
            for attr in ['payment_query', 'errand_pay_query', 'errand_link_query', 'payout_query']:
                setattr(self, attr, "SELECT 1 WHERE FALSE")
    
    def _setup_matching_rules(self):
        """设置匹配规则"""
        self.matching_cols_pay = ['extractReference', 'extractOtherNumber', 'extractDamageNumber']
        self.matching_cols_errand = ['isReference', 'damageNumber', 'invoiceReference', 'ocrNumber']
        self.base_url = 'https://admin.direktregleringsportalen.se/errands/'
    
    def execute_workflow(self, payments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        执行支付匹配工作流
        
        Args:
            payments: 支付列表
            
        Returns:
            Dict[str, Any]: 匹配结果
        """
        try:
            # 转换为DataFrame
            pay_df = pd.DataFrame(payments)
            
            if pay_df.empty:
                return {
                    "success": False,
                    "error": "支付列表为空",
                    "results": []
                }
            
            # 处理支付数据
            pay = self._process_payment(pay_df)
            pay = self._parse_info(pay)
            
            # 获取案件数据
            errand = self._get_errand_data()
            
            if not errand.empty:
                # 执行匹配流程
                pay = self._match_by_info(pay, errand)
                
                # 处理剩余未匹配项
                mask = pay['status'].isin(["No Found", ""])
                if mask.any():
                    pay = self._remainder_unmatched_amount(pay)
                
                # 实体和金额匹配
                mask = pay['status'].isin(["No Found", ""])
                if mask.any():
                    pay = self._match_entity_and_amount(pay, errand)
                
                # 支出匹配
                mask = pay['status'].isin(["No Found", ""])
                if mask.any():
                    payout = self._get_payout_data()
                    if not payout.empty:
                        pay = self._match_payout(pay, payout)
            
            # 格式化结果
            results = self._format_results(pay)
            statistics = self._calculate_statistics(pay)
            
            return {
                "success": True,
                "results": results,
                "statistics": statistics,
                "total_processed": len(payments)
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": f"支付匹配工作流执行失败: {str(e)}",
                "results": []
            }
    
    def _process_payment(self, pay: pd.DataFrame) -> pd.DataFrame:
        """处理支付数据"""
        try:
            pay['createdAt'] = pd.to_datetime(pay['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')

            # 提取参考号码
            ref_reg = reg.compile(r'\d+')
            pay.loc[pay['reference'].notna(), 'extractReference'] = pay.loc[pay['reference'].notna(), 'reference'].apply(
                lambda x: ''.join(ref_reg.findall(x)) if isinstance(x, str) else None
            )
            pay.loc[pay['extractReference'].notna(), 'extractReference'] = pay.loc[pay['extractReference'].notna(), 'extractReference'].replace('', None)
            
            # 初始化字段
            pay['settlementAmount'] = 0
            pay['status'] = ""

            # 初始化额外列
            for col in self.info_item_list:
                col_name = col.split('_')[1] if '_' in col else col
                if col_name not in pay.columns:
                    pay[col_name] = None
            
            init_columns = ['valPay', 'valErrand', 'isReference', 'insuranceCaseId', 'referenceLink']
            for col in init_columns:
                pay[col] = [[] for _ in range(len(pay))]
            
            return pay[['id', 'valPay', 'valErrand', 'amount', 'settlementAmount', 'isReference', 'insuranceCaseId', 'referenceLink',
                       'status', 'extractReference', 'extractDamageNumber', 'extractOtherNumber', 'bankName', 'info', 'reference', 'createdAt']]
        
        except Exception as e:
            print(f"处理支付数据失败: {str(e)}")
            return pay
    
    def _parse_info(self, pay: pd.DataFrame) -> pd.DataFrame:
        """解析支付信息"""
        try:
            mask = pay['info'].notna()
            for idx_pay, row_pay in pay[mask].iterrows():
                ic = self.bank_dict.get(row_pay['bankName'], 'None')
                mask_info = self.info_reg['item'].str.startswith(ic)
                
                for _, row_info_reg in self.info_reg[mask_info].iterrows():
                    col = row_info_reg['item'].split('_')[1] if '_' in row_info_reg['item'] else row_info_reg['item']
                    pattern = row_info_reg['regex']
                    compiled_pattern = reg.compile(pattern, reg.DOTALL | reg.IGNORECASE)
                    match = compiled_pattern.search(row_pay['info'])
                    
                    if match:
                        matched_value = match.group(1).strip()
                        if pd.isna(row_pay.get(col)):
                            pay.at[idx_pay, col] = matched_value
                        else:
                            pay.at[idx_pay, 'isReference'].append(matched_value)
            
            # 清理重复值
            pay.loc[pay['extractDamageNumber'] == pay['extractOtherNumber'], 'extractDamageNumber'] = None
            pay.loc[pay['extractReference'] == pay['extractOtherNumber'], 'extractOtherNumber'] = None
            pay.loc[pay['extractReference'] == pay['extractDamageNumber'], 'extractDamageNumber'] = None
            
            return pay
        except Exception as e:
            print(f"解析支付信息失败: {str(e)}")
            return pay
    
    def _get_errand_data(self) -> pd.DataFrame:
        """获取案件数据"""
        try:
            errand = fetchFromDB(self.errand_pay_query.format(CONDITION=""))
            if not errand.empty:
                errand['createdAt'] = pd.to_datetime(errand['createdAt'], utc=True).dt.tz_convert('Europe/Stockholm')
            return errand
        except Exception as e:
            print(f"获取案件数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _get_payout_data(self) -> pd.DataFrame:
        """获取支出数据"""
        try:
            return fetchFromDB(self.payout_query)
        except Exception as e:
            print(f"获取支出数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _find_matches(self, pay: pd.DataFrame, errand: pd.DataFrame, idx_pay: int, row_pay: pd.Series) -> List[int]:
        """查找匹配的案件"""
        matched = {col_pay: [] for col_pay in self.matching_cols_pay}
        
        for col_pay in self.matching_cols_pay:
            val_pay = row_pay.get(col_pay)
            if pd.notna(val_pay):
                val_amount = row_pay['amount']
                mask_errand_before_payment = errand['createdAt'] <= row_pay['createdAt']

                for _, row_errand in errand[mask_errand_before_payment].iterrows():
                    for col_errand in self.matching_cols_errand:
                        val_errand = row_errand.get(col_errand)
                        if pd.notna(val_errand) and (val_pay == val_errand):
                            if row_errand['isReference'] not in pay.at[idx_pay, 'isReference']:
                                pay.at[idx_pay, 'isReference'].append(row_errand['isReference'])
                                pay.at[idx_pay, 'valPay'].append(val_pay)
                                
                            if (val_amount == row_errand['settlementAmount']) and (int(row_errand['insuranceCaseId']) not in matched[col_pay]):
                                matched[col_pay].append(int(row_errand['insuranceCaseId']))
                                pay.at[idx_pay, 'valErrand'].append(val_errand)
        
        # 获取共同匹配的案件ID
        matched_lists = [set(matched[col_pay]) for col_pay in self.matching_cols_pay if matched[col_pay]]
        if matched_lists:
            matched_insurance_case_id = [id for id in matched_lists[0] if all(id in col for col in matched_lists[1:])]
            if not matched_insurance_case_id:
                matched_insurance_case_id = []
                for col in matched_lists:
                    for id in col:
                        if id not in matched_insurance_case_id:
                            matched_insurance_case_id.append(id)
        else:
            matched_insurance_case_id = []
        
        return matched_insurance_case_id
    
    def _generate_links(self, col_list1: List, col_list2: List, condition: str) -> List[str]:
        """生成链接"""
        links = []
        if len(col_list1) > 0:
            for id, val_errand in zip(col_list1, col_list2):
                if condition == 'ic.reference':
                    condition_str = f"{condition} = '{id}'"
                elif condition == 'ic.id':
                    condition_str = f"{condition} = {id}"
                else:
                    continue
                
                try:
                    result = fetchFromDB(self.errand_link_query.format(CONDITION=condition_str))
                    if not result.empty:
                        errand_number = result.iloc[0]['errandNumber']
                        ref = result.iloc[0]['reference']
                        link = f'<a href="{self.base_url}{errand_number}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by {val_errand}">{ref}</a>'
                        links.append(link)
                    else:
                        links.append(f'{id} (No Corresponding Link)')
                except:
                    links.append(f'{id} (Link Error)')
        
        return links
    
    def _match_by_info(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """通过信息匹配"""
        mask = (pay['info'].notna() | pay['extractReference'].notna())
        for idx_pay, row_pay in pay[mask].iterrows():
            matched_insurance_case_id = self._find_matches(pay, errand, idx_pay, row_pay)
            qty = len(matched_insurance_case_id)
            
            if qty > 0:
                pay.at[idx_pay, 'insuranceCaseId'].extend(matched_insurance_case_id)
                links = self._generate_links(row_pay['insuranceCaseId'], row_pay['valErrand'], 'ic.id')
                pay.at[idx_pay, 'referenceLink'] = links
                
                if qty == 1:
                    pay.at[idx_pay, 'status'] = f"One DR matched perfectly (reference: {', '.join(links)})."
                elif qty > 1:
                    pay.at[idx_pay, 'status'] = f"Found {qty} matching DRs (references: {', '.join(links)}) and the payment amount matches each one."
            else:
                pay.at[idx_pay, 'status'] = "No Found"

        return pay
    
    def _partly_amount_matching(self, ref_amount_dict: Dict, target_amount: float) -> Optional[List[str]]:
        """部分金额匹配"""
        references = list(ref_amount_dict.keys())
        amounts = list(ref_amount_dict.values())
        
        for r in range(1, len(amounts) + 1):
            for combo in combinations(zip(references, amounts), r):
                combo_references, combo_amounts = zip(*combo)
                if sum(combo_amounts) == target_amount:
                    return list(combo_references)
        
        return None
    
    def _remainder_unmatched_amount(self, pay: pd.DataFrame) -> pd.DataFrame:
        """处理剩余未匹配的金额"""
        mask = pay['status'].isin(["No Found", ""])
        
        for idx, row_pay in pay[mask].iterrows():
            matched_insurance_case_id, is_reference, links = [], [], []
            ref_amount_dict = {}
            msg = "No Found"
            
            # 收集所有参考号
            if len(row_pay['isReference']) > 0:
                is_reference.extend(row_pay['isReference'])
            
            for ref_field in ['extractReference', 'extractOtherNumber', 'extractDamageNumber']:
                if pd.notna(row_pay.get(ref_field)) and len(str(row_pay[ref_field])) == 10:
                    if str(row_pay[ref_field]) not in is_reference:
                        is_reference.append(str(row_pay[ref_field]))
            
            if len(is_reference) > 0:
                try:
                    condition = f"""AND ic.reference IN ({', '.join([f"'{ref}'" for ref in set(is_reference)])})"""
                    sub_errand = fetchFromDB(self.errand_pay_query.format(CONDITION=condition))
                    sub_errand.loc[sub_errand['settlementAmount'].isna(), 'settlementAmount'] = 0
                    
                    if not sub_errand.empty:
                        for _, row_sub in sub_errand.iterrows():
                            if row_sub['insuranceCaseId'] not in matched_insurance_case_id:
                                pay.loc[idx, 'settlementAmount'] += row_sub['settlementAmount']
                                matched_insurance_case_id.append(row_sub['insuranceCaseId'])
                                
                            if row_sub['isReference'] not in pay.at[idx, 'isReference']:
                                pay.at[idx, 'isReference'].append(row_sub['isReference'])
                                pay.at[idx, 'valPay'].append(row_sub['isReference'])
                                
                            if row_sub['isReference'] not in ref_amount_dict:
                                ref_amount_dict[row_sub['isReference']] = row_sub['settlementAmount']
                except:
                    pass
            
            ref_list = pay.at[idx, 'isReference']
            val_errand_list = pay.at[idx, 'valPay']
            links = self._generate_links(ref_list, val_errand_list, 'ic.reference')
            pay.at[idx, 'referenceLink'] = links
            
            qty = len(matched_insurance_case_id)
            if qty > 0:
                total_settlement_amount = pay.at[idx, 'settlementAmount']
                row_payment_amount = row_pay['amount']

                if row_payment_amount == total_settlement_amount:
                    if qty == 1:
                        msg = f"One DR matched perfectly (reference: {', '.join(links)})."
                    else:
                        msg = f"Found {qty} matching DRs (references: {', '.join(links)}), and the total amount matches the payment."
                else:
                    matched_references = self._partly_amount_matching(ref_amount_dict, row_payment_amount)
                    if matched_references:
                        matched_links = [link for link in links if any(ref in link for ref in matched_references)]
                        if len(matched_references) == 1:
                            msg = f"One DR matched perfectly (reference: {', '.join(matched_links)})."
                        else:
                            msg = f"Found {len(matched_references)} matching DRs (references: {', '.join(matched_links)}), and the total amount matches the payment."
                    else:
                        if qty == 1:
                            msg = f"Found 1 relevant DR (reference: {', '.join(links)}), but the amount does not match."
                        else:
                            msg = f"Found {qty} relevant DRs (references: {', '.join(links)}), but the amounts do not match."

            pay.at[idx, 'status'] = msg

        return pay
    
    def _match_entity_and_amount(self, pay: pd.DataFrame, errand: pd.DataFrame) -> pd.DataFrame:
        """匹配实体和金额"""
        def msg_for_one(one_line_df: pd.DataFrame, source: str, amount: float) -> str:
            errand_number = one_line_df.iloc[0]['errandNumber']
            ref = one_line_df.iloc[0]['isReference']
            
            if source == 'Insurance_Company':
                entity = one_line_df.iloc[0]['insuranceCompanyName']
            elif source == 'Clinic':
                entity = one_line_df.iloc[0]['clinicName']
            else:
                entity = 'Unknown'
                
            link = f'<a href="{self.base_url}{errand_number}" target="_blank" style="background-color: gray; color: white; padding: 2px 5px;" title="matched by Entity: {entity} and Amount: {amount}">{ref}</a>'
            
            if amount == one_line_df.iloc[0]['settlementAmount']:
                return f"One DR matched perfectly (reference: {link}) by both entity and amount."
            else:
                return "No Found"
        
        if self.payout_entity.empty:
            return pay
            
        payout_entity_source = self.payout_entity.set_index('payoutEntity')['source'].to_dict()
        ic_dict = self.payout_entity.loc[self.payout_entity['source'] == 'Insurance_Company'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
        clinic_dict = self.payout_entity.loc[self.payout_entity['source'] == 'Clinic'].groupby('payoutEntity')['clinic'].apply(list).to_dict()
        
        mask = pay['status'].isin(["No Found", ""])
        for idx, row_pay in pay[mask].iterrows():
            amount = row_pay['amount']
            source = payout_entity_source.get(row_pay['bankName'], "Unknown")
            
            entity_matched = pd.DataFrame()
            
            if source == 'Insurance_Company':
                ic_list = ic_dict.get(row_pay['bankName'], [])
                if ic_list:
                    entity_matched = errand.loc[
                        (errand['createdAt'] <= row_pay['createdAt']) & 
                        (errand['insuranceCompanyName'].isin(ic_list))
                    ]
            elif source == 'Clinic':
                clinic_list = clinic_dict.get(row_pay['bankName'], [])
                if clinic_list:
                    entity_matched = errand.loc[
                        (errand['createdAt'] <= row_pay['createdAt']) & 
                        (errand['clinicName'].isin(clinic_list))
                    ]
            
            if not entity_matched.empty:
                qty = entity_matched.shape[0]
                if qty == 1:
                    msg = msg_for_one(entity_matched, source, amount)
                    pay.at[idx, 'status'] = msg
            else:
                pay.at[idx, 'status'] = "No Found"

        return pay
    
    def _match_payout(self, pay: pd.DataFrame, payout: pd.DataFrame) -> pd.DataFrame:
        """匹配支出"""
        mask = pay['status'].isin(["No Found", ""])
        for idx, row_pay in pay[mask].iterrows():
            matched_trans_id, matched_clinic_name, matched_type = [], [], []
            
            for col in self.matching_cols_pay:
                val_pay = row_pay.get(col)
                if pd.notna(val_pay):
                    for _, row_payout in payout.iterrows():
                        if (val_pay == row_payout['reference']) and (row_pay['amount'] == row_payout['amount']):
                            if pd.notna(row_payout['transactionId']) and row_payout['transactionId'] not in matched_trans_id:
                                matched_trans_id.append(int(row_payout['transactionId']))
                            if pd.notna(row_payout.get('clinicName')) and row_payout['clinicName'] not in matched_clinic_name:
                                matched_clinic_name.append(row_payout['clinicName'])
                            if pd.notna(row_payout.get('type')) and row_payout['type'] not in matched_type:
                                matched_type.append(row_payout['type'])

            qty = len(matched_trans_id)
            if qty == 1:
                pay.at[idx, 'status'] = f"Payment has been paid out<br>TransactionId: {matched_trans_id[0]}<br>Amount: {row_pay['amount'] / 100:.2f} kr<br>Clinic Name: {matched_clinic_name[0]}<br>Type: {matched_type[0]}"
            elif qty > 0:
                pay.at[idx, 'status'] = f"Payment has been paid out {qty} times<br>TransactionId:{' '.join(map(str, matched_trans_id))}<br>Amount: {row_pay['amount'] / 100:.2f} kr<br>Clinic Name: {' '.join(map(str, matched_clinic_name))}<br>Type: {' '.join(map(str, matched_type))}"
            else:
                pay.at[idx, 'status'] = 'No matching DRs found.'

        return pay
    
    def _format_results(self, pay: pd.DataFrame) -> List[Dict[str, Any]]:
        """格式化结果"""
        # 格式化金额显示
        pay['amount'] = pay['amount'].apply(lambda x: f"{x / 100:.2f} kr")
        
        # 转换为响应格式
        results = []
        for _, row in pay.iterrows():
            results.append({
                'id': row['id'],
                'reference': row.get('reference', ''),
                'bankName': row.get('bankName', ''),
                'amount': row['amount'],
                'info': row.get('info', ''),
                'createdAt': str(row['createdAt']),
                'insuranceCaseId': row.get('insuranceCaseId', []),
                'status': row.get('status', '')
            })
        
        return results
    
    def _calculate_statistics(self, pay: pd.DataFrame) -> Dict[str, Any]:
        """计算统计信息"""
        total = len(pay)
        matched = len(pay[~pay['status'].str.contains('No Found|No matching DRs found', na=False)])
        perfect = len(pay[pay['status'].str.contains('One DR matched perfectly', na=False)])
        mul_perfect = len(pay[pay['status'].str.contains('each one', na=False)])
        payout = len(pay[pay['status'].str.contains('paid out', na=False)])
        one_total_match = len(pay[pay['status'].str.contains('total amount matches the payment', na=False)])
        no_found = len(pay[pay['status'].str.contains('No Found', na=False)])
        no_match = len(pay[pay['status'].str.contains('No matching DRs found', na=False)])
        
        return {
            "total_payments": total,
            "matched_payments": matched,
            "unmatched_payments": total - matched,
            "matching_rate": matched / total if total > 0 else 0,
            "perfect_matches": perfect,
            "multiple_perfect_matches": mul_perfect,
            "payout_matches": payout,
            "total_amount_matches": one_total_match,
            "no_found": no_found,
            "no_match": no_match
        }
    
    def batch_process_payments(self, payment_batches: List[List[Dict[str, Any]]]) -> Dict[str, Any]:
        """批量处理多个支付批次"""
        all_results = []
        all_errors = []
        total_processed = 0
        
        for i, payments in enumerate(payment_batches):
            try:
                result = self.execute_workflow(payments)
                if result['success']:
                    all_results.extend(result['results'])
                    total_processed += len(payments)
                else:
                    all_errors.append({
                        "batch_index": i,
                        "error": result['error']
                    })
            except Exception as e:
                all_errors.append({
                    "batch_index": i,
                    "error": str(e)
                })
        
        return {
            "results": all_results,
            "errors": all_errors,
            "total_processed": total_processed,
            "successful_batches": len(payment_batches) - len(all_errors),
            "failed_batches": len(all_errors)
        }
    
    def validate_payment_data(self, payment: Dict[str, Any]) -> Tuple[bool, str]:
        """验证支付数据"""
        required_fields = ['id', 'amount', 'reference', 'bankName', 'createdAt']
        
        for field in required_fields:
            if field not in payment:
                return False, f"缺少必填字段: {field}"
        
        if not isinstance(payment['id'], int) or payment['id'] <= 0:
            return False, "id必须是正整数"
        
        if not isinstance(payment['amount'], (int, float)) or payment['amount'] <= 0:
            return False, "amount必须是正数"
        
        if not payment['bankName'].strip():
            return False, "bankName不能为空"
        
        try:
            datetime.fromisoformat(payment['createdAt'].replace('Z', '+00:00'))
        except ValueError:
            return False, "createdAt时间格式无效"
        
        return True, "验证通过"
    
    def get_matching_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """获取匹配摘要"""
        if not results:
            return {"message": "无结果"}
        
        status_counts = {}
        for result in results:
            status = result.get('status', 'Unknown')
            # 简化状态分类
            if 'One DR matched perfectly' in status:
                simplified_status = 'Perfect Match'
            elif 'matching DRs' in status and 'total amount matches' in status:
                simplified_status = 'Total Amount Match'
            elif 'paid out' in status:
                simplified_status = 'Paid Out'
            elif 'No Found' in status:
                simplified_status = 'No Found'
            elif 'No matching DRs found' in status:
                simplified_status = 'No Match'
            else:
                simplified_status = 'Other'
                
            status_counts[simplified_status] = status_counts.get(simplified_status, 0) + 1
        
        return {
            "total_results": len(results),
            "status_breakdown": status_counts,
            "most_common_status": max(status_counts, key=status_counts.get) if status_counts else None
        }
