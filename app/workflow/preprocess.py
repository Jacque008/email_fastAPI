from __future__ import annotations
from dataclasses import dataclass, field
import pandas as pd
from ..services.services import DefaultServices
from ..dataset.email_dataset import EmailDataset

@dataclass
class PreProcess:
    services: DefaultServices = field(default_factory=DefaultServices)
    # RETURN_COLS: Sequence[str] = (
    #     'id','date','from','originSender','sender','source','to','originReceiver','receiver','sendTo',
    #     'clinicCompType','subject','origin','email','attachments','category','totalAmount','settlementAmount',
    #     'errandId','reference','insuranceCaseRef','insuranceNumber','damageNumber','animalName','ownerName',
    # )
    # 'id', 'date', 'from', 'parsedFrom', 'source', 'originSender', 'sender',
    # 'to', 'parsedTo', 'sendTo', 'originReceiver','receiver',
    # 'subject', 'textPlain', 'textHtml', 'attachments', 'origin', 'email',
    # 'clinicCompType', 'capturedIc', 
    # 'reference', 'insuranceCaseRef', 'errandId', 'errandDate', 'errand_matched', 'connectedCol', 'category',  
    # 'damageNumber', 'insuranceNumber', 'isStaffAnimal', 'showPage', 'note', 
    # 'paymentOption', 'strategyType', 
    # 'folksamOtherAmount', 'totalAmount', 'attach_totalAmount', 'attach_settlementAmount', 'settlementAmount', 
    # 'animalName_Sveland', 'attach_animalName', 'animalName', 'attach_ownerName', 'ownerName'
    # RETURN_COLS: Sequence[str] = (
        #     'id','from','sender','source','to',
        #     'receiver','sendTo','clinicCompType',
        #     'reference','insuranceCaseRef','errandId','category',
        #     
        # )

    def do_preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        preProcess：
          adjust_time_format -> detect_sender -> generate_email_content
          -> detect_receiver -> vendor specials -> alias -> sort -> return
        """
        emails = EmailDataset(df=df, services=self.services)
        (emails
            .adjust_time()              
            .detect_sender()
            .generate_content()  
            .detect_receiver()
            .handle_vendor_specials()
            .sort_by_date(ascending=True)
        )
        
        out = emails.to_frame()

        return out