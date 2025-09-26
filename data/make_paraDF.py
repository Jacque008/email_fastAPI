import pandas as pd

local_folder = "data/para_tables"
def make_df(dict,filename):
    df = pd.DataFrame(dict)
    df['id'] = range(1, len(df) + 1)
    columns = ['id'] + [col for col in df.columns if col != 'id']
    df = df[columns]
    df.to_csv(f"{local_folder}/{filename}.csv",index=False)
    return None

#  ****************  1_1_7. IC_List  ****************
name = [
    'Svedea',
    'Folksam',
    'If',
    'Sveland',
    'Agria',
    'Moderna Försäkringar',
    'Dina Försäkringar',
    'ICA Försäkring',
    'Many Pets',
    'Gefvert',
    'Mjöbäcks Pastorat Hästförsäkring',
    'Lassie',
    'Trygg-Hansa',
    'Hedvig',
    'Dunstan',
    'Petson',
    'Furrychamp',
    'Varsam',
    'Wisentic',
    'Wisentic',
    'Drp',
    'Drp',
    'Drp',
    'Drp',
    'Drp',]
reference = [
    'svedea',
    'folksam',
    'if',
    'sveland',
    'agria',
    'moderna',
    'dina',
    'ica',
    'manypets',
    'gefvert',
    'mjoback',
    'lassie',
    'trygghansa',
    'hedvig',
    'dunstan',
    'petson',
    'furrychamp',
    'varsam',
    'wisentic',
    'djurskador@djurskador.se',
    'mail@direktregleringsportalen.se',
    'support@direktregleringsportalen.se',
    '@drp.se',
    '@apoex.se',
    'swedbank-pay-balance-report',]
address = [
    'direktreglering@svedea.se',
    'folksam.edjur@wisentic.com',
    'djurskador@if.se',
    'direktreglering@sveland.se',
    'direktregleringsmadjur@agria.se',
    'direktreglering@modernadjurforsakringar.se',
    'dir.hundkatt@dina.se',
    'direktreglering.djur@ica.se',
    'direktreglering@manypets.com',
    'info@gefvert.se',
    'ulf.litzell@elmoleather.com',
    'direktreglering@lassie.co',
    'direktreglering@trygghansa.se',
    'direktreglering@hedvig.com',
    'hastdir@dunstan.se',
    'direktreglering@petson.se',
    'direktreglering@furrychamp.com',
    'direktregleringvarsam@crawco.se',
    None,
    None,
    None,
    None,
    None,
    None,
    None,]
ic_dict = {'insuranceCompany': name, 
           'insuranceCompanyReference':reference,
           'forwardAddress': address}
# make_df(ic_dict, "ic")

#  ****************  1_2_7. Clinic  _List  ****************

#  ****************  1_3_7. Stop_Words  ****************
stopWords_dict = {
    'stopWords': [
        r'---------- Forwarded message ---------',
        r'---------- Vidarebefordrat meddelande ---------',
        r'<!doctype html>',
        r'\[?Trygg-Hansa_Logo_RGB_Pos.jpg\]?\s+www\.trygghansa\.se<http:\/\/www\.trygghansa\.se>\s+Trygg-Hansa Försäkring filial\s+106 26 Stockholm',
        r'Bolagsverket org\.nr 516403\–8662\s+Filial till Tryg Forsikring A\/S Erhvervsstyrelsen CVR-nr 24260666\s+Klausdalsbrovej 601 DK-2750 Ballerup Danmark',
        r'OBS! Detta mejl går inte att svara på, var vänliga och skicka till djurdirekt@folksam\.se Har ni frågor\? Om ni har frågor är ni välkomna att kontakta oss på telefon[\d\s-]+\. Detta telefonnummer är endast till för er djurkliniker, så att ni snabbt kan komma fram till oss\.',
        r'Växel:[\d\s-]+www\.sveland\.se\s*Postadress: Sveland Djurförsäkringar, Box 199, 221 00 Lund',
        r'\[cid:image00\d\.png@[0-9A-Z]+\.[0-9A-Z]+\] 0771-[\d\s]+\[cid:image00\d\.png@[0-9A-Z]+\.[0-9A-Z]+\] svedea\.se',
        r'\[cid:image00\d\.png@[0-9A-Z]+\.[0-9A-Z]+\] Dina Försäkring AB',
        r'Lapplands Djurklinik\nLuleå\n\[https:\/\/cdn2\.hubspot\.net\/hubfs\/53\/tools\/email\-signature\-generator\/icons\/phone\-icon\-2x\.png\]',
        r'010-4509850<tel:010-4509850>\n\[https:\/\/cdn2\.hubspot\.net\/hubfs\/53\/tools\/email-signature-generator\/icons\/email\-icon\-2x\.png\]\nlulea@lapplandsdjurklinik\.se<mailto:lulea@lapplandsdjurklinik\.se>',
        r'Telefon:[\d\s-]+E-post: djurskador@trygghansa\.se<mailto:djurskador@trygghansa\.se> | FE 380 | SE-106 56 Stockholm',
        r'(E-post|Mejl): djurskador@if\.se Telefon:[\d\s-]+Vi behandlar våra kunders personuppgifter i enlighet med dataskyddsförordningen och övrig dataskydds- och försäkringslagstiftning',
        r'Vi behandlar våra kunders personuppgifter i enlighet med dataskyddsförordningen och övrig dataskydds\- och försäkringslagstiftning',
        r'Har du några frågor eller vill diskutera ärendet (?:är du välkommen att|kan du) ringa telefonnummer\s+[\d \-]+eller skicka (?:mejl|epost) till djurskador@if\.se',
        r'Har ni frågor\?\s*Om ni har frågor är ni välkomna att kontakta oss på telefon ',
        r'Öppettider: Vardagar 8\.00-17\.00\. Direktregleringen stänger 17\.00 Stängt torsdagar 09\.00-10\.00 Vid frågor kan ni ringa[\d\s-]+',
        r'Wisentic support Körsbärsvägen 5 114 23 Stockholm http:\/\/www\.wisentic\.com\/ Vänliga Hälsningar Supporten',
        r'Teamet på Hedvig\s*Ⓗ\sHedvig AB\sBirger Jarlsgatan 57\sSE\-113 56 Stockholm, SWE',
        r'([\d\s-]+)?djurskador@folksam\.se<mailto:djurskador@folksam\.se> ([\d\s-]+)?Folksam Djurskador Box 735 851 21 Sundsvall',
        r'[\d\s-]+\s*djurskador@folksam\.se\<mailto:djurskador@folksam\.se\>\s*Folksam Djurskador\s*Box\s*735\s*851\s*21 Sundsvall\s*www\.folksam\.se\<http:\/\/www\.folksam\.se\/\>',
        r'Folksam Djurskador\s*djurskador@folksam\.se\<mailto:djurskador@folksam\.se\>\s*[\d\s-]+',
        r'Gratis rådgivning för din[ \wåöä,]+med djurförsäkring i Folksam',
        r'Telefon:[\d\s-]+Mejl: Djurskador@ica\.se<mailto:Djurskador@ica\.se> ICA Försäkring',
        r'Teamet på ManyPets\. ---------------------------------------------- Direktregleringsportalen',
        r'Information Ifs telefontider.\s+',
        r'\[Warning\] - This is an external email, please be cautious when opening links and attachments!',
        r'Skickat från Outlook',
        r'Från:.*\nSkickat:.*\nTill:.*\nÄmne:.*(?=\n)', 
        r'Från:.*\nDatum:.*\nTill:.*\nÄmne:.*(?=\n)',
        r'From:.*\nSent:.*\nTo:.*\nSubject:.*(?=\n)', 
        r'Direktregleringsportalen\s*Ser mailet konstigt ut\? Klicka här',
        r'(?<=\n)Den[\d\wåöä\-:\.,+(kl) ]+skrev[\wÅÖÄåöä\-\.\s]+(DRP)? *(<.*@.*>)?:?(?=\n)',
        r'(?<=\n)På[\d\wåöä\-: ]+CEST skrev\s+[\wÅÖÄåöä\-\.\s]+(DRP)?\s*(<.*@.*>)?:?(?=\n)',
        r'(?<=\n)[\d\wåöä\-:\. ]+skrev[\wÅÖÄåöä\-\.\s]+DRP\s*(<.*@.*>)?:(?=\n)',
        r'(?<=\n)[\d\-:\. ]+skrev[\wÅÖÄåöä\-\.\s]+(DRP|Direktregleringsportalen):(?=\n)', 
        r'(?<=\n)Den[\wåöä\d:<>@\.\s]+skrev:(?=\n)',
        r'(?<=\n)On .*? wrote:',
        r'\sHälsningar,?\s',
        r'\s(Vänligen|Hälsar),?\n',
        r'\sVänligen,\s',
        ]}
# make_df(stopWords_dict, "stopWords")

#  ****************  1_4_7. Forward_Words  ****************
forwardWords_dict = {
    'forwardWords': [
        r'Från:.*\nSkickat:.*\nTill:.*\nÄmne:.*(?=\n)', 
        r'^\s*Från:.*\n\s*Skickat:.*\n\s*Till:.*\n\s*Ämne:.*(?=\n|$)',
        r'Från:.*\nDatum:.*\nTill:.*\nÄmne:.*(?=\n)',
        r'^\s*Från:.*\n\s*Datum:.*\n\s*Till:.*\n\s*Ämne:.*(?=\n)',
        r'From:.*\nSent:.*\nTo:.*\nSubject:.*(?=\n)', 
        r'^\s*From:.*\n\s*Sent:.*\n\s*To:.*\n\s*Subject:.*(?=\n)', 
        r'(?<=\n)Den[\d\wåöä\-:\.,+(kl) ]+skrev[\wÅÖÄåöä\-\.\s]+(DRP)? *(<.*@.*>)?:?(?=\n)'
        r'(?<=\n)På[\d\wåöä\-: ]+CEST skrev\s+[\wÅÖÄåöä\-\.\s]+(DRP)?\s*(<.*@.*>)?:?(?=\n)',
        r'(?<=\n)[\d\wåöä\-:\. ]+skrev[\wÅÖÄåöä\-\.\s]+DRP\s*(<.*@.*>)?:(?=\n)',
        r'(?<=\n)[\d\-:\. ]+skrev[\wÅÖÄåöä\-\.\s]+(DRP|Direktregleringsportalen):(?=\n)',         
        r'(?<=\n)Den[\wåöä\d:<>@\.\s]+skrev:(?=\n)',
        r'(?<=\n).*skrev.*(?=\n)',
        r'(?<=\n)On .*? wrote:',
        r'---------- Forwarded message ---------',
        r'---------- Vidarebefordrat meddelande ---------',
        ]
    }
# make_df(forwardWords_dict, "forwardWords")

#  ****************  1_5_7. Forward_Trimming_Template   ****************
action = [
  # Trim
    'Trim',  
    'Trim',
    'Trim', # 3
    
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim', # 10
    
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim', # 7
    
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim', # 10
    
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim',
    'Trim', # 7
  # Other
    'Forward_Address',
    'Forward_Subject',
    'Forward_Subject',
    'Subject',
    'ProvetCloud_Msg',
    'ProvetCloud_Clinic',
    'ProvetCloud_Recipient',
    'Wisentic_Msg',
  # Template
    'Wisentic_Error_Subject',
    'Wisentic_Error_Template',
    'Insurance_Validation_Error_Subject',
    'Insurance_Validation_Error_Template',
    
    'Complement_DR_Insurance_Company_Subject',
    'Complement_DR_Insurance_Company_Template',
    'Complement_DR_Clinic_Subject',
    'Complement_DR_Clinic_Template',
    'ProvetCloud_Template',
    
    'Complement_Damage_Request_Insurance_Company_Subject',
    'Complement_Damage_Request_Insurance_Company_Template',
    'Complement_Damage_Request_Clinic_Subject',
    'Complement_Damage_Request_Clinic_Template',
    
    'Settlement_Request_Subject',
    'Settlement_Request_Template',
    'Settlement_Approved_Subject',
    'Settlement_Approved_Template',
    
    'Message_Subject',
    'Message_Template',
    'Question_Subject',
    'Question_Template',
    ]
templates = [
  # Trim
    # 3
    r'Om du har några frågor angående detta mejl, är du välkommen att kontakta oss på Sveland', # sveland
    r'Svara till djurskador@if.se vid eventuella frågor om detta mejl.', # if
    r'Wisentic AB, Körsbärsvägen 5,\n114 23 Stockholm, Sweden', # svedea
    
    # 10
    r'\(detta mejl går inte att svara på då hamnar det i fel e\-postkorg\)', # folksam
    r'Obs! Vi sparar inte underlag vi ej kan åtgärda\/reglera', # folksam
    r'OBS!! Vi sparar ej underlag vi ej kan handlägga', # folksam
    r'Svara gärna tillbaka i denna tråd', # komp_DR_IC/sveland
    r'Har du några frågor eller vill diskutera ärendet är du välkommen att ringa telefonnummer', # komp_DR_IC/if
    r'Vi vill göra er uppmärksamma på att If, enligt försäkringsvillkoret, har rätt att ta del av djurets journaler', # komp_SA_IC/if
    r'Har ni några frågor eller vill diskutera ärendet är (ni|du) välkomna att ringa',
    r'Märk med Komplettering av direktreglering', # komp_DR_IC/folksam
    r'Vi vore tacksamma om ni kunde mejla det till folksam\.edjur@wisentic\.com', # komp_DR_IC/folksam
    r'OBS! Detta mejl går inte att svara på, var vänliga och skicka till folksam.edjur@wisentic\.com',  # komp_DR_IC/folksam
    # r'Var vänlig uppge vårt ärendenummer [\d ]+ vid all kontakt med oss', # komp_DR_IC/if
    
    # 7
    r'Vi önskar (er|dig) en (trevlig|fortsatt bra)?\s*dag',
    r'Ser fram emot ert svar',
    r'Tack på förhand', # komp_DR_IC/Trygg-Hansa
    r'Ha en (fortsatt )?fin dag',
    r'Ha det fint!',
    r'Vänligen återkom så kikar vi vidare på reglering',
    r'Vi återkommer så fort vi fått svar',
    
    # 10
    r'\n(?:Med|Varma)\s+(?:vänliga?|Vänliga?)?\s+(?:hälsning|Hälsning|hälsningar|Hälsningar)?,?', # komp_DR_IC/lassie
    r'\n(?:vänliga?|Vänliga?)\s+(?:hälsninga?r?|Hälsninga?r?)?,?', # komp_DR_IC/lassie
    r'\n(Hälsning|Hälsningar|Häslningar|Trevlig helg|Best regards),?\s',
    r'\n(Vänligen|Hälsar),?\n',
    r'\nVänligen,\s',
    r'\n(M|m)[V|v](H|h),?\s', # Komp_DR_Clinic/ica
    r'\n\n/ [A-ZÅÖÄ]',
    r'Agnes Rudin',
    r'Teamet på Hedvig',
    r'Djurskador\s+Telefon: 033-78 60 502',
    
    # 7
    r'----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------',
    r'Aniuma Vetcentrum AB\nOrg\.nr:559425\-6603\nMagasinstgatan 9A\n903 27 Umeå', # Komp_DR_Clinic/Aniuma Vetcentrum AB
    r'Receptionen\nDjurvårdare nivå 2', # Komp_DR_Clinic/Enköpings Djurklinik
    r'Tel: 046\-260 55 00\s+Mail: hej@nordicvet\.net\s+www\.nordicvet\.net', # Komp_DR_Clinic/NordicVet
    r'/Veterinär Ina Lindblom', # Komp_DR_Clinic/Sjöbo Djurklinik
    r'Oljevägen 11', # Komp_DR_Clinic/Lapplands Djurklinik Gällivare
    r'(?:\[cid:[\w-]+\]\s+)?Veterinär Kämpaslätten\s+Galjalyckevägen 5', # Komp_DR_Clinic/Veterinär Kämpaslätten
  # Other  8
    r'Vidarebefordrat meddelande [\-]+\s*(?:Från|From):\s*(.*?)\s*(?:Datum|Date):',
    r'(?i)^Märk(?!.*betalningen).*?med:?\s*(.*?)(?=\n|$)',
    r'Det går bra att svara på detta mejl eller till djurskador@svedea\.se, (?i)märk mejlsvaret med\s+(.+?)\s*(?=\s+eller\b|[.,;:!?]|[\r\n]|$)',
    r'(?:\b(?:SV:|RE:)\s*\b)*(.*)',
    r'\nMeddelande: (.*?)\nDu hittar patienthistoriken som en bifogad PDF fil',
    r'\[BODY\](.*?)\s+Logo',
    r'Vänligen vidarebefordra till ((?!försäkringsbolaget)[a-zåöä ]+?)\n*Mvh?',
    r'Ämne:.*?\n(.*?)\nDatum:.*?Från:.*?Till:.*?Ämne:',
  # Template  
        # 'Wisentic_Error_Subject'
        r'Info från försäkringsbolaget: Direktreglering {REFERENCE} behöver kompletteras', 
        # 'Wisentic_Error_Template'
        r'Hej,<br><br>Här kommer information om en direktreglering som inte kan hanteras av försäkringsbolaget. Dubbelkolla försäkringsuppgifterna med DÄ, och uppdatera i journalsystemet om aktuellt. Skicka de rätta uppgifterna till oss så uppdaterar vi ärendet och skickar om det till {WHO}.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Om ni inte får tag på nya försäkringsuppgifter har ni två alternativ:<br><li>vi sätter 0 kr ersättning (då betalar DÄ faktura + direktregleringsavgift i DRP)</li><li>vi avbryter direktregleringen (då driver ni själva in pengar från DÄ utanför DRP)</li><br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'Insurance_Validation_Error_Subject'
        r'Info från försäkringsbolaget: Direktreglering {REFERENCE} behöver kompletteras',
        # 'Insurance_Validation_Error_Template'
        r'Hej,<br><br>Här kommer information om en direktreglering som inte kan hanteras av försäkringsbolaget. Dubbelkolla försäkringsuppgifterna med DÄ, och uppdatera i journalsystemet om aktuellt. Skicka de rätta uppgifterna till oss så uppdaterar vi ärendet och skickar om det till {WHO}.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Om ni inte får tag på nya försäkringsuppgifter har ni två alternativ:<br><li>vi sätter 0 kr ersättning (då betalar DÄ faktura + direktregleringsavgift i DRP)</li><li>vi avbryter direktregleringen (då driver ni själva in pengar från DÄ utanför DRP)</li><br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        
        # 'Complement_DR_Insurance_Company_Subject'
        r'Info från försäkringsbolaget: Direktreglering {REFERENCE} behöver kompletteras',
        # 'Complement_DR_Insurance_Company_Template'
        r'Hej,<br><br>Här kommer önskemål om komplettering av en direktreglering. Svara på detta mail eller skicka ett mail med kompletteringen till {REFERENCE} så vidarebefordrar vi det till {WHO}.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'Complement_DR_Clinic_Subject'
        r'Komplettering av direktreglering {REFERENCE}',
        # 'Complement_DR_Clinic_Template'
        r'Hej,<br><br>Här kommer svar på efterfrågad komplettering från {WHO}.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Hör gärna av er om det behövs ytterligare komplettering. Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'ProvetCloud_Template'
        r'Hej,<br><br>Här kommer svar på efterfrågad komplettering från {WHO}.<br>Hör gärna av er om det behövs ytterligare komplettering. Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',

      
        # 'Complement_Damage_Request_Insurance_Company_Subject'
        r'Info från försäkringsbolaget: Komplettering av en skadeanmälan',
        # 'Complement_Damage_Request_Insurance_Company_Template'
        r'Hej,<br><br>Här kommer önskemål om komplettering av en skadeanmälan. Skicka kompletteringen till försäkringsbolaget med angiven referens. Ni kan också maila den till oss - ange då nedan angiven referens i svaret så vidarebefordrar vi det. <br><br>"<b><i>{EMAIL}</i></b>"<br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'Complement_Damage_Request_Clinic_Subject'
        r'Komplettering av en skadeanmälan',
        # 'Complement_Damage_Request_Clinic_Template'
        r'Hej,<br><br>Här kommer svar på efterfrågad komplettering från kliniken. <br><br>"<b><i>{EMAIL}</i></b>"<br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        
        # 'Settlement_Request_Subject'
        r'Info från försäkringsbolaget: Förhandsbesked',
        # 'Settlement_Request_Template'
        r'Hej,<br><br>Här kommer ett förhandsbesked från försäkringsbolaget.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'Settlement_Approved_Subject'
        r'Ersättningsbesked för DR utanför DRP?',
        # 'Settlement_Approved_Template'
        r'Hej,<br><br>Här kommer ett ersättningsbesked från {WHO} för en direktreglering som inte verkar vara skickad via DRP.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Återkoppla gärna ifall något ska meddelas försäkringsbolaget eller om vi missuppfattat.<br><br>Tack på förhand!<br><br><br>Mvh {ADMIN}<br>DRP',
        
        # 'Message_Subject'
        r'Information om ärende från {WHO}',
        # 'Message_Template'
        r'Hej,<br><br>Vänligen se meddelande från {WHO}.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Ni kan svara på detta mail om ni vill att vi vidarebefordrar något. Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
        # 'Question_Subject'
        r'Fråga om ärende från {WHO}',
        # 'Question_Template'
        r'Hej,<br><br>Här kommer en fråga om en direktreglering från {WHO}. Svara på detta mail {REFERENCE} så vidarebefordrar vi det till försäkringsbolaget.<br><br>"<b><i>{EMAIL}</i></b>"<br><br>Tack på förhand!{INFO}<br><br><br>Mvh {ADMIN}<br>DRP',
            ]
sugg_dict = {
          'action': action,
          'templates': templates} 
# make_df(sugg_dict, "forwardSuggestion")

#  ****************  1_6_7. Clinic_Complement_Type  ****************
complement_dict = {
    'complement': [
        r'Vi har mottagit en (direktreglering|skadeanmälan)',
        r'Här kommer(?: ett)? önskemål om komplettering av en (direktreglering|skadeanmälan)',
        r'Här kommer information om en (direktreglering|skadeanmälan)',
        r'Här kommer frågan om en (direktreglering|skadeanmälan)',
        r'gjorde en (direktreglering|skadeanmälan)',
        r'Tack för er förfrågan om (direktreglering|skadeanmälan)',
        ]
    }
# make_df(complement_dict, "clinicCompType")

#  ****************  1_7_7. Queries *****
queryDict = {
  'errandConnect': [''' SELECT DISTINCT 
                            er.id AS "errandId" ,
                            er.reference AS "errandNumber" ,
                            er."createdAt" AS "date" ,
                            fb."name" AS "insuranceCompany" ,
                            c."name" AS "clinicName" ,
                            ROUND((sums."totalAmount" + 50) / 100) AS "totalAmount",
                            ls."settlementAmount" , 
                            ic.reference ,
                            ic."insuranceNumber" ,
                            ic."damageNumber" ,
                            er."invoiceReference" ,
                            a."name" AS "animalName" , 
                            COALESCE(ao."firstName", '') || ' ' || COALESCE(ao."lastName", '') AS "ownerName" ,
                            er."paymentOption" ,
                            er."strategyType" ,
                            es.settled AS "settled" 
                        FROM insurance_case ic 
                        INNER JOIN errand er ON ic."errandId" = er.id
                        INNER JOIN animal a  ON ic."animalId" = a.id
                        INNER JOIN animal_owner ao ON er."animalOwnerId" = ao.id
                        INNER JOIN errand_status es ON er."statusId" = es.id
                        INNER JOIN ( SELECT 
                                        "insuranceCaseId", 
                                        SUM(amount) AS "totalAmount" 
                                    FROM insurance_case_billing_row
                                    GROUP BY "insuranceCaseId"
                                  ) sums ON ic.id = sums."insuranceCaseId"
                        LEFT JOIN ( SELECT DISTINCT 
                                        ist."insuranceCaseId",
                                        ROUND(ist."settlementAmount" / 100, 2) AS "settlementAmount"
                                    FROM insurance_settlement ist
                                    WHERE ist."updatedAt" = (
                                          SELECT MAX(inst."updatedAt")
                                          FROM insurance_settlement inst
                                          WHERE inst."insuranceCaseId" = ist."insuranceCaseId")
                                    ) AS ls ON ls."insuranceCaseId" = ic.id
                        INNER join insurance_company_email ice ON ic."insuranceCompanyEmailId" = ice.id
                        INNER JOIN insurance_company fb ON ice."insuranceCompanyId" = fb.id 
                        INNER JOIN clinic c ON er."clinicId" = c.id
                        WHERE {CONDITION}
                        ORDER BY date DESC;'''],
  'emailSpec': [''' SELECT DISTINCT 
                        e.id,
                        e."createdAt" ,
                        e."from" ,
                        e."to" ,
                        e.subject ,
                        e."textPlain" ,
                        e."textHtml" ,
                        e."object" ->> 'Attachments' AS "attachments"
                    FROM email e
                    WHERE e.id = {EMAILID};'''],
  'errandInfo':['''  SELECT DISTINCT 
                              er.id AS "errandId" ,
                              ic.reference ,
                              c."name" AS "clinicName" ,
                              fb."name" AS "insuranceCompany"
                          FROM insurance_case ic 
                          JOIN errand er ON er.id = ic."errandId" 
                          JOIN clinic c ON c.id = er."clinicId" 
                          JOIN insurance_company_email ice ON ice.id = ic."insuranceCompanyEmailId" 
                          JOIN insurance_company fb ON fb.id = ice."insuranceCompanyId" 
                          WHERE {COND} ;'''],
  'forwardSummaryInfo': ['''SELECT DISTINCT 
                                e.id,
                                COALESCE(e."errandId", (ecr."data" -> 'errandId'->> 0)::int) AS "errandId" ,
                                ic.reference ,
                                er."invoiceReference" ,
                                COALESCE(ecr."correctedCategory", ecr."category") AS "correctedCategory" ,
                                ecr."data" ->> 'insuranceNumber' AS "insuranceNumber" ,
                                ecr."data" ->> 'damageNumber' AS "damageNumber" ,
                                ecr."data" ->> 'animalName' AS "animalName" ,
                                ecr."data" ->> 'ownerName' AS "ownerName" ,
                                ecr."data" ->> 'sender' AS "sender" ,
                                COALESCE(ecr."data" ->> 'receiver', ecr."data" ->> 'recipient') AS "receiver" ,
                                c."linkJournalTenant" ,
                                ic."journalNumber" 
                            FROM email e
                            JOIN email_category_request ecr ON ecr."emailId" = e.id
                            JOIN clinic c ON ( (((ecr."data" ->> 'source') = 'Clinic') 
                                   AND (c."name" = (ecr."data" ->> 'sender')))
                                 OR (((ecr."data" ->> 'source') = 'Insurance_Company') 
                                   AND (c."name" = COALESCE(ecr."data" ->> 'receiver', ecr."data" ->> 'recipient'))))
                            LEFT JOIN errand er ON e."errandId" = er.id
                            LEFT JOIN insurance_case ic ON ic."errandId" = er.id 
                            WHERE {CONDITION};'''],
  'payment': [''' SELECT DISTINCT
                      id, 
                      amount , 
                      reference , 
                      "rawBg"->>'info' AS "info" ,
                      "rawBg"->>'name' AS "bankName",
                      "createdAt"
                  FROM bankgiro_payment_file_line bpfl
                  WHERE id = {ID}
                  ORDER BY "createdAt" DESC;'''],
  'errandPay': [''' SELECT DISTINCT
                        er.id AS "errandId",
                        er."createdAt" ,
                        er.reference AS "errandNumber" ,
                        ic.id AS "insuranceCaseId" ,
                        ic.reference AS "isReference",
                        ls."settlementAmount", 
                        ic."damageNumber" ,
                        er."invoiceReference" ,
                        er."ocrNumber" ,
                        c."name" AS "clinicName" ,
                        fb."name" AS "insuranceCompanyName" ,
                        ic."animalId" 
                    FROM errand er
                    JOIN errand_status es ON er."statusId" = es.id
                    JOIN insurance_case ic ON ic."errandId" = er.id
                    JOIN clinic c ON er."clinicId" = c.id 
                    JOIN insurance_company_email ice ON ic."insuranceCompanyEmailId" = ice.id 
                    JOIN insurance_company fb ON ice."insuranceCompanyId" = fb.id
                    JOIN (SELECT DISTINCT 
                                is2."insuranceCaseId",
                                is2."settlementAmount"
                            FROM insurance_settlement is2
                            WHERE is2."updatedAt" = (
                                SELECT MAX(is3."updatedAt")
                                FROM insurance_settlement is3
                                WHERE is3."insuranceCaseId" = is2."insuranceCaseId")
                          ) AS ls ON ls."insuranceCaseId" = ic.id
                    WHERE es.complete IS FALSE
                    ORDER BY er."createdAt" DESC;'''],
  'partialPay': [''' SELECT DISTINCT
                      er.id AS "errandId",
                      er."createdAt" ,
                      ic.id AS "insuranceCaseId" ,
                      ic.reference AS "isReference",
                      ls."settlementAmount",
                      tl."createdAt" AS "paymentReceivedTime" ,
                      ABS(tl.amount) AS "paymentFromFB"
                  FROM errand er
                  JOIN insurance_case ic ON ic."errandId" = er.id
                  JOIN (SELECT DISTINCT 
                              is2."insuranceCaseId",
                              is2."settlementAmount"
                          FROM insurance_settlement is2
                          WHERE is2."updatedAt" = (
                              SELECT MAX(is3."updatedAt")
                              FROM insurance_settlement is3
                              WHERE is3."insuranceCaseId" = is2."insuranceCaseId")
                        ) AS ls ON ls."insuranceCaseId" = ic.id
                  JOIN "transaction" t ON t."errandId" = er.id
                  JOIN transaction_line tl ON tl."transactionId" = t.id
                  JOIN account a ON a.id = tl."accountId"
                  WHERE t.type_ IN ('settlement_payment','settlement') AND
                        (t.type_ = 'settlement_payment'
                        OR (t.type_ = 'settlement' AND NOT EXISTS ( 
                            SELECT 1
                            FROM "transaction" tt
                            WHERE tt."errandId" = t."errandId"
                              AND tt.type_ = 'settlement_payment')))
                        AND a."ownerType" = 'insurance_company'
                        {CONDITION}
                  ORDER BY er."createdAt" DESC;'''],
  'errandLink': ['''  SELECT DISTINCT
                          ic.id,
                          er.reference AS "errandNumber",
                          ic.reference
                      FROM insurance_case ic 
                      INNER JOIN errand er ON ic."errandId" = er.id
                      WHERE {CONDITION};'''],
  'payout': ['''  SELECT DISTINCT
                    bpfl.id ,
                    bpfl."createdAt" AS "createdAt" ,
                    bpfl.reference ,
                    bpfl."transactionId" ,
                    bpfl.amount	,
                    c."name"  AS "clinicName" ,
                    t.type_ AS "type"
                  FROM bankgiro_payout_file_line bpfl 
                  INNER JOIN "transaction" t ON t.id = bpfl."transactionId" 
                  INNER JOIN errand er ON er.id = t."errandId" 
                  INNER JOIN clinic c  ON c.id = er."clinicId"
                  WHERE bpfl."createdAt" >= NOW() - INTERVAL '2 month'
                  AND bpfl.type_ = 'paid'
                  ORDER BY bpfl."createdAt" DESC;'''],
  'summaryChat': ['''SELECT DISTINCT 
                  ic.reference ,
                  c.type_ ,
                  cm."createdAt" ,
                  cm.message,
                  cm."fromAdminUserId"  ,
                  cm."fromClinicUserId" ,
                  c."clinicId" ,
                  cl."name" AS "clinicName",
                  cm."fromInsuranceCompanyId" ,
                  c."insuranceCompanyId" ,
                  fb.name AS "insuranceCompanyName" 
              FROM chat_message cm
              INNER JOIN chat c ON c.id = cm."chatId"
              INNER JOIN errand er ON er.id = c."errandId" 
              INNER JOIN insurance_case ic ON ic."errandId" = c."errandId"
              LEFT JOIN clinic cl ON cl.id = c."clinicId"
              LEFT JOIN insurance_company fb ON fb.id = c."insuranceCompanyId"
              WHERE {CONDITION}
              ORDER BY cm."createdAt" ;'''],
  'summaryEmail': [''' SELECT 
                          e.id,
                          e."createdAt",
                          e.subject,
                          e."textPlain",
                          e."textHtml",
                          e."from",
                          e."to",
                          e."errandId",
                          e.folder ,
                          ecr."data" ->> 'sender' AS "sender",
                          ecr."data" ->> 'recipient' AS "receiver"
                      FROM email e
                      LEFT JOIN email_category_request ecr ON e.id = ecr."emailId"
                      INNER JOIN errand er ON e."errandId" = er.id
                      INNER JOIN insurance_case ic ON ic."errandId" = er.id
                      WHERE {CONDITION}
                      ORDER BY e."createdAt" ASC;'''],
  'summaryComment': [''' SELECT 
                              cr."emailId" AS "id", 
                              cr."errandId" ,
                              c.id AS "commentId",
                              c."createdAt" ,
                              c.content AS "content",
                              CASE 
                                  WHEN cr."emailId" IS NOT NULL THEN 'Email'
                                  WHEN cr."errandId" IS NOT NULL THEN 'Errand'
                              END AS "type"
                          FROM comment c
                          INNER JOIN comment_relation cr ON c.id = cr."commentId"
                          LEFT JOIN email e ON cr."emailId" = e.id
                          LEFT JOIN errand er ON cr."errandId" = er.id
                          LEFT JOIN insurance_case ic ON ic."errandId" = cr."errandId"
                          WHERE c."content" IS NOT NULL
                            AND {CONDITION}
                          ORDER BY c."createdAt" ASC;'''],
  'info': ['''  SELECT DISTINCT 
                    e.id,
                    ecr."timestamp",
                    ecr."data" ->> 'sender' AS "sender" ,
                    ecr."data" ->> 'recipient' AS "receiver" ,
                    ecr."data" ->> 'source' AS "source" ,
                    ecr.category ,
                    ecr."correctedCategory" ,
                    e.subject 
                FROM email_category_request ecr 
                LEFT JOIN email e ON ecr."emailId" = e.id 
                WHERE {CONDITION}
                    AND (ecr.category = 'Information' OR ecr."correctedCategory" = 'Information')
                    LIMIT 1;'''],
  'logBase': [''' SELECT  
                    er.id AS "errandId" ,
                    er.reference AS "errandNumber" ,
                    er."createdAt" AS "errandCreaTime" ,
                    ic.id AS "insuranceCaseId" ,
                    ic.reference ,
                    ls."settlementAmount" ,
                    ls."updatedAt" AS "updatedTime" ,
                    er."clinicId" ,
                    c."name" AS "clinicName" ,
                    ic.id AS "insuranceCompanyId" ,
                    ic2."name" AS "insuranceCompanyName" ,
                    ic."insuranceEmailLastSent" AS "sendTime" ,
                    es.complete 
                  FROM errand er
                  INNER JOIN insurance_case ic ON ic."errandId" = er.id
                  LEFT JOIN ( SELECT DISTINCT ist."insuranceCaseId", ROUND(ist."settlementAmount" / 100, 2) AS "settlementAmount" , ist."updatedAt"
                              FROM insurance_settlement ist
                              WHERE ist."updatedAt" = (
                                    SELECT MAX(inst."updatedAt")
                                    FROM insurance_settlement inst
                                    WHERE inst."insuranceCaseId" = ist."insuranceCaseId")
                              ) AS ls ON ls."insuranceCaseId" = ic.id
                  JOIN clinic c ON c.id = er."clinicId" 
                  JOIN insurance_company_email ice ON ice.id = ic."insuranceCompanyEmailId" 
                  JOIN insurance_company ic2 ON ic2.id = ice."insuranceCompanyId" 
                  JOIN errand_status es ON es.id = er."statusId" 
                  WHERE {COND}
                  ORDER BY er."createdAt" DESC'''],
  'logEmail': ['''SELECT  
                      er.id AS "errandId" ,
                      e.id AS "emailId",
                      e."createdAt" AS "emailTime" ,
                      e.subject ,
                      e."textPlain" ,
                      e."textHtml" ,
                      ecr.category ,
                      ecr."correctedCategory" ,
                      ecr."data" ->> 'source' AS "source" 
                  FROM errand er             
                  INNER JOIN email e ON e."errandId" = er.id
                  INNER JOIN email_category_request ecr ON ecr."emailId" = e.id
                  WHERE e.folder = 'inbox'
                    AND {COND}
                  ORDER BY er."createdAt" DESC'''],
  'logChat': ['''SELECT  
                        er.id AS "errandId" ,
                        cm.id AS "chatMessageId" ,
                        c.type_ AS "chatType",
                        cm."createdAt" AS "chatTime" ,
                        au."firstName" AS "chatDRP" ,
                        cm.message ,
                        --cm."fromClinicUserId" AS "chatClinic" ,
                        clinic."name" AS "chatClinic" ,
                        --cm."fromInsuranceCompanyId" AS "chatFB" ,
                      	fb."name" AS "chatFB"
                    FROM errand er  
                    INNER JOIN chat c ON c."errandId" = er.id
                    INNER JOIN chat_message cm ON cm."chatId" = c.id
                    LEFT JOIN clinic_user cu  ON cu.id = cm."fromClinicUserId" 
                    LEFT JOIN clinic ON clinic.id = cu."clinicId" 
                    LEFT JOIN insurance_company fb ON fb.id = cm."fromInsuranceCompanyId" 
                    LEFT JOIN admin_user au ON cm."fromAdminUserId" = au.id
                    WHERE {COND}
                    ORDER BY er."createdAt" DESC'''],
  'logComment': ['''SELECT  
                        er.id AS "errandId" ,
                        cr."commentId" ,
                        cr."createdAt" AS "commentTime" ,
                        c2."content" ,
                        c2."createdByAdminId" ,
                        au2."firstName" AS "commentDRP" 
                    FROM errand er 
                    INNER JOIN comment_relation cr ON cr."errandId" = er.id
                    INNER JOIN "comment" c2 ON c2.id = cr."commentId" 
                    LEFT JOIN admin_user au2 ON c2."createdByAdminId" = au2.id
                    WHERE {COND}
                    ORDER BY er."createdAt" DESC'''],
  'logOriginalInvoice': ['''SELECT  
                                t."errandId" ,
                                t.id AS "transactionId" ,
                                t."createdAt" AS "transTime" ,
                                ROUND(tl.amount / 100, 2) AS "invoiceAmount" 
                            FROM "transaction" t 
                            JOIN errand er on er.id = t."errandId"
                            JOIN transaction_line tl ON tl."transactionId" = t.id
                            JOIN account a on tl."accountId" = a.id
                            WHERE t.type_ = 'original_invoice'
                                AND a."ownerType" = 'animal_owner'
                                AND er.id = 73060
                            ORDER BY er."createdAt" DESC;'''],
  'logInvoiceSP': ['''SELECT  
                          t."errandId" ,
                          t.id AS "transactionId" ,
                          t."createdAt" AS "transTime" ,
                          t."paymentOption" ,
                          spo."invoiceNumber"  ,
	                        spo."invoiceAmount"
                      FROM "transaction" t
                      INNER JOIN errand er on er.id = t."errandId"
                      INNER JOIN (SELECT sp.id, 
                                         sp."payeeReference" AS "invoiceNumber",
                                         ROUND(CAST(sp."extSwedbankPayOrder" -> 'paymentOrder' ->> 'amount' AS NUMERIC) / 100, 2) AS "invoiceAmount" 
                                  FROM swedbank_pay_order sp) AS spo ON spo.id = t."swedbankPayOrderId"
                      WHERE t.type_ = 'customer_invoice'
                        AND t."paymentOption" = 'swedbankPay'
                        AND {COND}
                      ORDER BY er."createdAt" DESC'''],
  'logInvoiceFO': ['''SELECT  
                          t."errandId" ,
                          t.id AS "transactionId" ,
                          t."createdAt" AS "transTime" ,
                          t."paymentOption" ,
                          f."invoiceNumber",
	                        f."invoiceAmount"
                      FROM "transaction" t
                      INNER JOIN errand er on er.id = t."errandId"
                      INNER JOIN (SELECT fo.id, 
                                         fo."fortusInvoiceNumber" AS "invoiceNumber",
                                         fo."orderData" ->> 'order_amount' AS "invoiceAmount"  
                                  FROM fortus_order fo) AS f ON f.id = t."fortusOrderId"
                      WHERE t.type_ = 'customer_invoice'
                        AND t."paymentOption" = 'fortus'
                        AND {COND}
                      ORDER BY er."createdAt" DESC'''],
  'logInvoiceKA': ['''SELECT  
                          t."errandId" ,
                          t.id AS "transactionId" ,
                          t."createdAt" AS "transTime" ,
                          t."paymentOption" ,
                          ka."invoiceNumber" ,
                          ka."invoiceAmount" 
                      FROM "transaction" t 
                      INNER JOIN errand er ON t."errandId" = er.id 
                      INNER JOIN (  SELECT DISTINCT 
                                        t.id AS "id",
                                        t.reference AS "invoiceNumber" , 
                                        ROUND(tl.amount / 100, 2) AS "invoiceAmount" 
                                    FROM "transaction" t
                                    INNER JOIN transaction_line tl ON tl."transactionId" = t.id
                                    INNER JOIN account a on tl."accountId" = a.id
                                    WHERE a."ownerType" = 'animal_owner') AS ka ON ka."id" = t."customerPaymentId"
                      WHERE t.type_ = 'customer_invoice'
                        AND t."paymentOption" not in ('fortus','swedbankPay')
                        AND {COND}
                      ORDER BY er."createdAt" DESC'''],
  'logReceive': ['''SELECT 
                        t."errandId" ,
                        t.id AS "transactionId" ,
                        tl."createdAt" ,
                        ROUND(tl.amount / 100, 2) AS "amount",
                        a."name" ,
                        tl."accountingDate"
                    FROM  transaction_line tl
                    INNER JOIN account a ON tl."accountId" = a.id
                    INNER JOIN "transaction" t ON tl."transactionId" = t.id
                    INNER JOIN errand er on er.id = t."errandId"
                    INNER JOIN errand_status es ON es.id = er."statusId" 
                    WHERE {COND}
                    ORDER BY er."createdAt" DESC'''], 
  'logCancel': [''' SELECT
                        t."errandId" ,
                        t.id AS "transactionId" ,
                        t."createdAt" AS "cancelTime"
                    FROM "transaction"  t               
                    INNER JOIN errand er ON er.id = t."errandId" 
                    INNER JOIN errand_status es ON es.id = er."statusId" 
                    WHERE t.type_ = 'cancel'
                      AND es.cancelled IS TRUE
                      AND {COND}
                    ORDER BY er."createdAt" DESC '''],
  'logRemoveCancel': [''' SELECT
                              t."errandId" ,
                              t.id AS "transactionId" ,
                              t."createdAt" AS "removeTime"
                          FROM "transaction"  t
                          INNER JOIN errand er ON er.id = t."errandId" 
                          WHERE t.type_ = 'remove_cancel'
                            AND {COND}
                          ORDER BY er."createdAt" DESC '''],
  'admin': [''' SELECT id, email, "firstName"
                FROM admin_user au
                WHERE {COND}; '''],
  'updateClinicEmail': [''' SELECT DISTINCT 
                                c.id AS "clinicId",
                                c."name" AS "clinicName",
                                c."email" AS "clinicEmail", 
                                'main_email' AS "role",
                                c.inactivated 
                            FROM clinic c
                            UNION 
                            SELECT DISTINCT 
                                c.id AS "clinicId",
                                c."name" AS "clinicName",
                                c."functionEmail" AS "clinicEmail",
                                'function_email' AS "role",
                                c.inactivated 
                            FROM clinic c
                            UNION
                            SELECT DISTINCT 
                                c.id AS "clinicId",
                                c."name" AS "clinicName",
                                cu.email AS "clinicEmail",
                                'employee_email' AS "role",
                                c.inactivated 
                            FROM clinic c
                            JOIN clinic_user cu ON cu."clinicId" = c.id
                            WHERE cu.email IS NOT NULL
                              AND cu.email <> c."email"
                              AND cu.email <> c."functionEmail"
                              AND cu.active = TRUE 
                              AND cu.email NOT IN ('mail@direktregleringsportalen.se', 'support@direktregleringsportalen.se')
                              AND cu.email NOT ILIKE '%@drp.se'
                              AND cu.email NOT ILIKE '%@apoex.se'
                            ORDER BY "clinicId";'''],
  }
make_df(queryDict, "queries") 

#  ****************  2_1_3. Category Regex  ****************
category = [ 
  # 'Auto_Reply' # 14
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply', 
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply', # 10
    'Auto_Reply', 
    'Auto_Reply', 
    'Auto_Reply',
    'Auto_Reply',
  # 'Finance_Report', # 2
    'Finance_Report',
    'Finance_Report', # 2
  # 'Wisentic_Error', # 6
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error', # 5
    'Wisentic_Error',
    'Wisentic_Error',
  # 'Other', # 2
    'Other',
    'Other',
  # 'Information', # 9
    'Information',
    'Information',
    'Information',
    'Information', 
    'Information', # 5
    'Information',
    'Information',
    'Information',
    'Information',
  # 'Complement', # 40
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 5
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 10
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 15
    'Complement',
    'Complement',
    'Complement',
    'Complement',  
    'Complement', # 20
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 25
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 30
    'Complement',
    'Complement',
    'Complement', 
    'Complement',
    'Complement', # 35
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 40
  # 'Insurance_Validation_Error', # 9
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
  # 'Settlement_Denied', # 26
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', 
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', # 10
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', # 15
    'Settlement_Denied', 
    'Settlement_Denied', 
    'Settlement_Denied', 
    'Settlement_Denied',
    'Settlement_Denied', # 20
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', 
    'Settlement_Denied', # 25
    'Settlement_Denied',
  # 'Settlement_Approved', # 13
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', # 5
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', # 10
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', 
  # 'Complement_Reply', # 11
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply', 
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply', 
    'Complement_Reply', # 10
    'Complement_Reply', 
  # 'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request', # 5
  # 'Message', # 19
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 5
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 10
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 15
    'Message',
    'Message',
    'Message', 
    'Message',
  # 'Question', #17
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 5
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 10
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 15
    'Question', 
    'Question', 
  # 'Spam' # 9
    'Spam',
    'Spam',
    'Spam',
    'Spam',
    'Spam', # 5
    'Spam',
    'Spam',
    'Spam',
    'Spam',
    ]
regex = [
  # Auto_Reply # 14
     r'(autosvar|automatic reply).*?\n\[body\]',
     r'ärendet kommer att regleras manuellt av handläggaren på', 
     r'ärendet kommer regleras manuellt av handläggaren på if', 
     r'automatisk direktreglering är tyvärr inte möjligt för denna faktura\. vi återkommer med beslut', 
     r'direktregleringen kunde ej göras automatiskt\. ärendet är sparat hos oss svar kommer så fort en handläggare granskat ärendet', 
     r'tack för ditt mejl\. vi tar hand om ditt ärende så snart vi kan', 
     r'vi har tagit emot direktregleringen och hanterarden så snart vi kan\. du är välkommen att ringa oss på 010-410 70 57 om du har några frågor', 
     r'tack för ditt mejl\W\s*vi svarar så snart vi har möjlighet',
     r'kommer att svara på ditt email inom \d+ timmar',
     r'återkommer till er inom \d+ arbetsdagar',
     r'återkommer så snart vi fått in all information vi behöver',
     r'kommer att hantera ditt ärende så snart vi kan',
     r'vi har fått in dom underlag vi behöver och kommer att hantera ditt ärende inom kort',
     r'tack för ditt email! \- thank you for your email!',
  # Finance_Report # 2
     r'här kommer underlag avseende avräkning rubricerad vecka',
     r'your balance report and transaction statistics with accounting number',
  # Wisentic_Error # 6
     r'diagnoskod saknas\. vänligen kontrollera och skicka om hela underlaget\. registrerat journalsystem är drp',
     r'ingen gällande försäkring finns\.\.? registrerat journalsystem är drp',
     r'försäkringsnumret har fel format\.',
     r'tyvärr kan ingen gällande försäkring hittas på det angivna numret\. vänligen kontrollera sifferkombinationen och\/eller om det saknas någon siffra',
     r'försäkringsnumret stämmer inte eller är felaktigt inskrivet i pdf-filen\. vänligen kontrollera och skicka om hela underlaget',
     r'gällande försäkring saknas på angivet',
     r'underlaget saknar information om diagnoser registrerat journalsystem är drp',
  # Other # 2
    r'emails processed spf or dkim aligned spf and dkim not aligned',
    r'\[subject\].*?byte till drp.*?\n\[body\]',
  # Information # 9
    r'\[subject\].*?(öppettid|stäng|driftstörning).*?\n\[body\]',
    r'tekniskt problem i vårt system',
    r'se meddelande från svedea nedan',
    r'hjälp oss att bli bättre',
    r'vi saknar ert svar',
    r'driftstörning(ar)? i vårt system',
    r'fungera som det ska',
    r'rate your conversation',
    r'påminnelse om viktig information',
  # Complement', # 40 / 24-10-6
    r'beställning av förtydligande av journal',
    r'beställning av journal(kopia)?',
    r'beställning av specificerade kostnader',
    r'för att gå vidare i ärendet har vi bett djurägaren inkomma med',
    r'behöver ett förtydligande kunna svara på denna reglering',
    r'önskar komplettering för att kunna svara på denna reglering',
    r'önskar att ni specificerar kostnader',
    r'undrar om ni kan skicka fullständig journal(kopia)? till oss',
    r'vänligen återkom med färdigskriven journal',
    r'vänligen komplettera med en fullständig journal',
    r'(behöver|behöva|ta del av)\s*(en)?\s*(komplett|fullständig)?\s* journal(kopia)?',
    r'vi skulle behöva få hjälp med nedanstående information från er',
    r'vi skulle vilja ha fullständig journal',
    r'för att [\wåöä ]* (behöva|behöver)',
    r'skulle ni kunna skicka kvitto för besöket den \d\d\d\d-\d\d-\d\d på',
    r'vi behöver.*?för att kunna svara på denna reglering',
    r'vi behöver specade kostnader för',
    r'behöver se [\wåöä ]* innan vi svarar på denna förfrågan',
    r'vi önskar (få )?veta',
    r'innan vi kan svara på denna regleringen behöver vi få veta',
    r'behöv(er|a) (en )?(få|ha|vet(a)?|remiss|komplettering|be att få|fullständig|chipnr|kostnad|röntgen|färdigskriven)',
    r'be er återkomma med färdigskriven journal',
    r'fanns det någon kostnad',
    r'vi skulle behöva [\wåöä ]* för att kunna hantera ärendet',
    # Complement_DRP_Insurance_Company # 10
      # r'angående direktregleringen med datum', # folksam
    r'märk med komplettering av direktreglering', # folksam
    r'vi behöver ett förtydligande innan vi kan direktreglera', # if
    r'vi behöver.*?innan vi kan direktreglera', # if
    r'för att vi ska kunna hjälpa till med direktreglering behöver vi få', # if
    r'vi har mottagit en direktreglering gällande', # svedea
    r'för att vi ska kunna fortsätta handlägga skadeärendet behöver vi ytterligare komplettering', # svedea
    r'för vidare reglering av ärendet behöver vi ta del av', # lassie
    r'vi behöver nedanstående komplettering', # sveland
    r'inväntar komplettering från kunden för att kunna reglera', # dina
    r'här kommer önskemål om komplettering av en direktreglering',
    # Complement_Damage_Request_Insurance_Company # 6
    r'vi har tagit emot en (skadeanmälan|livskadeanmälan) från vår kund gällande [a-zåöä\d\-\.\s]*(undrar om ni kan|behöver veta)', # folksam
    r'för att kunna ta ställning i ersättningsfrågan.*?är vi tacksamma om', # if
    r'vi har fått in en ersättningsansökan från en kund som varit med sitt djur hos er\. vi skulle behöva få hjälp med nedanstående information från er', # sveland
    r'vi önskar ta del av fullständig journal', # svedea
    r'här kommer önskemål om komplettering av en skadeanmälan',
    r'märk gärna med:?.*?och FF\d+S\s',
  # Insurance_Validation_Error # 9
    r'manypets erbjuder inte längre djurförsäkring i sverige och vi direktreglerar inte längre skador',
    r' ingen[\wåöä ]+(försäkring|namn|person|pn) ',
    r'(?<= inte)[\wåöä ]+hitta[\wåöä ]+',
    r'hittar [\wåöä ]* ingen',
    r' saknas på angivet ',
    r'ingen gällande försäkring på behandlingsdatum',
    r'vi kan tyvärr inte finna någon försäkring här hos oss på kundens namn\/ adress',
    r'tyvärr ingen match med något av personnumret eller försäkringsnumret',
    r'försäkringsnumret är inte korrekt',
  # Settlement_Denied # 26
    r'dessvärre kan vi inte lämna ersättning för', 
    r'inte lämna någon ersättning i detta ärende',
    r'nekad ersättning pga', 
    r'(kan inte|saknar möjlighet att) direktreglera besök äldre än 7 dagar',
    r'tyvärr kan vi inte hjälpa till med direktreglering av följande skäl:', 
    r'inte kan ersätta denna skada',
    r'kan inte direktreglera detta (besök|underlag)',
    r'kommer därför inte lämna någon ersättning just nu',
    r'måste tyvärr meddela att de ersättningsbara kostnaderna för detta besök understiger den fasta självrisken i djurets försäkring',
    r'vår direktreglering gällande [a-zåöä]+ ser ut som följande: totalt fakturabelopp:[\d\s\-,]+kr nekad direktreglering, kund får inkomma med skadeanmälan',
    r'kan tyvärr inte genomföra direktregleringen på',
    r'inte direktreglera denna kostnad',
    r'kan vi tyvärr inte direktreglera',
    r'försäkringen kommer inte kunna lämna ersättning för detta besök',
    r'ingen ersättning av dagens kvitto',
    r'direktreglering gällande [\p{L}\(\)\\\/\*\.\'"´, -]+ kan inte utföras',
    r'saknar möjlighet att direktreglera besök äldre än 7 dagar',
    r'\s0\s*kr ersättning',
    r'belopp att utbetala 0,00\s*kr',
    r'ersättning klinik:\s*0',
    r'ersättning: 0\s*kr',
    r'if betalar er 0\s*kronor',
    r'sveland betalar er 0\s*kronor',
    r'total ersättning som utbetalas: 0(\s*kr|,-|:-)',
    r'utbetalt belopp: 0\s*kr',
    r'vi nekar direktreglering gällande',
  # Settlement_Approved # 14
    r'- belopp att ersätta :', 
    r'belopp att utbetala', 
    r'bifogar ersättningsbrevet nedan för', 
    r'ersättning klinik:', 
    r'ersättning att betala ut :', # 5
    r'här kommer ersättningsbrevet', 
    r'här kommer sammanställningen', 
    r'här kommer svar på begärd direktreglering gällande', 
    r'total ersättning som utbetalas:', 
    r'utbetalt belopp:', # 10
    r'if betalar:?\s*[\d ,]+\s+kr', 
    r'vänligen notera försnr \d+', 
    r'vi (ersätter|ers) [\d\s]+(kr|:-)', 
    r'sveland betalar er', 
  # Complement_Reply', # 11
    r'du hittar patienthistoriken som en bifogad pdf fil',
    r'här kommer efterfrågad journal',
    r'här kommer efterfrågat kvitto',
    r'här kommer efterfrågat kvitto\/specifikation',
    r'här kommer efterfråga',
    r'här kommer fullständig journal',
    r'här kommer (journal(kopia)?|jk)',
    r'här kommer[\wåöä ]*(journal|kvitto|specifikation|journalkopia|jk|faktura)',
    r'här kommer svar på efterfrågad komplettering från kliniken',
    r'vänligen se bifogad journalkopia kliniken skickat angående nedanstående ärende',
    r'sätta 0 kr ersättning',
  # Settlement_Request # 5
    r'gällande förfrågan på djur:',
    r'ersätter [a-zåäö\s\/]+upp till försäkringens maxbelopp',
    r'ersätter ejkostnad för kastreringen',
    r'vi har mottagit ett förhandsbesked på',
    r'vi har tagit emot er förfrågan (gällande|angående)',
  # Message # 19
    r'avlivningskostnader ersätts inte\. men jag avslutar försäkringen',
    r'den här försäkringen är inte aktiv hos oss',
    r'det går bra att återbetala till',
    r'det här försäkringsnumret är inte i det formatet som våra försäkrinsnummer',
    r'det verkar som vi redan fått denna info',
    r'detta fnr tillhör ej oss och hittar endast en annan hund på angett pnr',
    r'detta svar har inte kommit in via api:et',
    r'e\-djur kan tyvärr inte hantera hästskador',
    r'jag hittade försäkringen med hjälp av den förra direktregleringen och hanterar den direkt',
    r'om du vill och kan vore det bra att få en lista på alla kliniker som ni har',
    r'se bilaga i denna mejltråd',
    r'vet inte om ni fått svar men direktreglering är nekad',
    r'vi har nu mottagit komplettering från kund och skickat beslutet om ersättning via direktregleringsportalen',
    r'vi har tyvärr inte kvittot\. kan det stå på mannen kanske',
    r'utbetald till fel klinik',
    r'betalningen är (nu )?stoppad och ni kan bortse från det ersättningsbesked som har skickats',
    r'har vidarebefordrat denna dir till svedea\. de kunder som tidigare haft ica-försäkring genom svedea ligger kvar hos dem och regleras av dem',
    r'klick här för att ange ett lösenord',
    r'kunden behöver godkänna detta innan vi kan gå vidare med direktregleringen',
  # Question' # 17
    r'\*vill kund att vi räknar med tidigare besök',
    r'\s*vi skulle vilja veta',
    r'\s+vill bara dubbelkolla med er att',
    r'\s+är det [^,.!;:]*\?',
    r'\s+hur[^,.!;:]*\?',
    r'\s+kan [^,.!;:]*\?',
    r'\s+när [^,.!;:]*\?',
    r'\s+ska [^,.!;:]*\?',
    r'\s+undrar om ni kan [^,.!;:]*\?',
    r'\s+vad [^,.!;:]*\?',
    r'\s+vem [^,.!;:]*\?',
    r'\s+vill ni [^,.!;:]*\?',
    r'\s+heter [^,.!;:]*\?',
    r'\s+(vilka|vilken) [^,.!;:]*\?',
    r'\s+har du möjlighet att [^,.!;:]*\?',
    r'\s+avser besöket något för [^,.!;:]*\?',
    r'\s+visst [^,.!;:]*\?',
  # Spam # 9
    r'transcript: america this week',
    r'\b(klubb|sex|älskare|träffas|hemlighet|dejting|impotent|lokal\sdating|bli\smedlem|coupon)\b',
    r'en ny plats på webben för diskreta överenskommelser',
    r'\so\strog',
    r'gratis prenumeration',
    r'socialt nätverk',
    r'vip[\-| ]kund',
    r'grupper[\wåöäÅÖÄ]* sex[\wåöäÅÖÄ]*',
    r' (kvinna|resebyråer|) ',
     ]
regex_dict = {
    'category': category,
    'regex': regex}
# make_df(regex_dict, "categoryReg")

category = [ 
  # 'Auto_Reply' # 14
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply', 
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply',
    'Auto_Reply', # 10
    'Auto_Reply', 
    'Auto_Reply', 
    'Auto_Reply',
    'Auto_Reply',
  # 'Finance_Report', # 2
    'Finance_Report',
    'Finance_Report', # 2
  # 'Wisentic_Error', # 6
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error',
    'Wisentic_Error', # 5
    'Wisentic_Error',
    'Wisentic_Error',
  # 'Other', # 2
    'Other',
    'Other',
  # 'Information', # 9
    'Information',
    'Information',
    'Information',
    'Information', 
    'Information', # 5
    'Information',
    'Information',
    'Information',
    'Information',
  # 'Complement', # 40
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 5
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 10
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 15
    'Complement',
    'Complement',
    'Complement',
    'Complement',  
    'Complement', # 20
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 25
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 30
    'Complement',
    'Complement',
    'Complement', 
    'Complement',
    'Complement', # 35
    'Complement',
    'Complement',
    'Complement',
    'Complement',
    'Complement', # 40
  # 'Insurance_Validation_Error', # 9
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
    'Insurance_Validation_Error',
  # 'Settlement_Denied', # 26
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', 
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', # 10
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', # 15
    'Settlement_Denied', 
    'Settlement_Denied', 
    'Settlement_Denied', 
    'Settlement_Denied',
    'Settlement_Denied', # 20
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied',
    'Settlement_Denied', 
    'Settlement_Denied', # 25
    'Settlement_Denied',
  # 'Settlement_Approved', # 13
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', # 5
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', # 10
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved',
    'Settlement_Approved', 
  # 'Complement_Reply', # 11
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply', 
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply',
    'Complement_Reply', 
    'Complement_Reply', # 10
    'Complement_Reply', 
  # 'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request',
    'Settlement_Request', # 5
  # 'Message', # 19
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 5
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 10
    'Message',
    'Message',
    'Message',
    'Message',
    'Message', # 15
    'Message',
    'Message',
    'Message', 
    'Message',
  # 'Question', #17
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 5
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 10
    'Question',
    'Question',
    'Question',
    'Question',
    'Question', # 15
    'Question', 
    'Question', 
  # 'Spam' # 9
    'Spam',
    'Spam',
    'Spam',
    'Spam',
    'Spam', # 5
    'Spam',
    'Spam',
    'Spam',
    'Spam',
    ]
regex = [
  # Auto_Reply # 14
     r'(?:autosvar|automatic reply).*?\n\[body\]',
     r'ärendet kommer att regleras manuellt av handläggaren på', 
     r'ärendet kommer regleras manuellt av handläggaren på if', 
     r'automatisk direktreglering är tyvärr inte möjligt för denna faktura\. vi återkommer med beslut', 
     r'direktregleringen kunde ej göras automatiskt\. ärendet är sparat hos oss svar kommer så fort en handläggare granskat ärendet', 
     r'tack för ditt mejl\. vi tar hand om ditt ärende så snart vi kan', 
     r'vi har tagit emot direktregleringen och hanterarden så snart vi kan\. du är välkommen att ringa oss på 010-410 70 57 om du har några frågor', 
     r'tack för ditt mejl\W\s*vi svarar så snart vi har möjlighet',
     r'kommer att svara på ditt email inom \d+ timmar',
     r'återkommer till er inom \d+ arbetsdagar',
     r'återkommer så snart vi fått in all information vi behöver',
     r'kommer att hantera ditt ärende så snart vi kan',
     r'vi har fått in dom underlag vi behöver och kommer att hantera ditt ärende inom kort',
     r'tack för ditt email! \- thank you for your email!',
  # Finance_Report # 2
     r'här kommer underlag avseende avräkning rubricerad vecka',
     r'your balance report and transaction statistics with accounting number',
  # Wisentic_Error # 6
     r'diagnoskod saknas\. vänligen kontrollera och skicka om hela underlaget\. registrerat journalsystem är drp',
     r'ingen gällande försäkring finns\.\.? registrerat journalsystem är drp',
     r'försäkringsnumret har fel format\.',
     r'tyvärr kan ingen gällande försäkring hittas på det angivna numret\. vänligen kontrollera sifferkombinationen och\/eller om det saknas någon siffra',
     r'försäkringsnumret stämmer inte eller är felaktigt inskrivet i pdf-filen\. vänligen kontrollera och skicka om hela underlaget',
     r'gällande försäkring saknas på angivet',
     r'underlaget saknar information om diagnoser registrerat journalsystem är drp',
  # Other # 2
    r'emails processed spf or dkim aligned spf and dkim not aligned',
    r'\[subject\].*?byte till drp.*?\n\[body\]',
  # Information # 9
    r'\[subject\].*?(?:öppettid|stäng|driftstörning).*?\n\[body\]',
    r'tekniskt problem i vårt system',
    r'se meddelande från svedea nedan',
    r'hjälp oss att bli bättre',
    r'vi saknar ert svar',
    r'driftstörning(?:ar)? i vårt system',
    r'fungera som det ska',
    r'rate your conversation',
    r'påminnelse om viktig information',
  # Complement', # 40 / 24-10-6
    r'beställning av förtydligande av journal',
    r'beställning av journal(?:kopia)?',
    r'beställning av specificerade kostnader',
    r'för att gå vidare i ärendet har vi bett djurägaren inkomma med',
    r'behöver ett förtydligande kunna svara på denna reglering',
    r'önskar komplettering för att kunna svara på denna reglering',
    r'önskar att ni specificerar kostnader',
    r'undrar om ni kan skicka fullständig journal(?:kopia)? till oss',
    r'vänligen återkom med färdigskriven journal',
    r'vänligen komplettera med en fullständig journal',
    r'(?:behöver|behöva|ta del av)\s*(?:en)?\s*(?:komplett|fullständig)?\s* journal(?:kopia)?',
    r'vi skulle behöva få hjälp med nedanstående information från er',
    r'vi skulle vilja ha fullständig journal',
    r'för att [\wåöä ]* (?:behöva|behöver)',
    r'skulle ni kunna skicka kvitto för besöket den \d\d\d\d-\d\d-\d\d på',
    r'vi behöver.*?för att kunna svara på denna reglering',
    r'vi behöver specade kostnader för',
    r'behöver se [\wåöä ]* innan vi svarar på denna förfrågan',
    r'vi önskar (?:få )?veta',
    r'innan vi kan svara på denna regleringen behöver vi få veta',
    r'behöv(?:er|a) (?:en )?(?:få|ha|vet(?:a)?|remiss|komplettering|be att få|fullständig|chipnr|kostnad|röntgen|färdigskriven)',
    r'be er återkomma med färdigskriven journal',
    r'fanns det någon kostnad',
    r'vi skulle behöva [\wåöä ]* för att kunna hantera ärendet',
    # Complement_DRP_Insurance_Company # 10
      # r'angående direktregleringen med datum', # folksam
    r'märk med komplettering av direktreglering', # folksam
    r'vi behöver ett förtydligande innan vi kan direktreglera', # if
    r'vi behöver.*?innan vi kan direktreglera', # if
    r'för att vi ska kunna hjälpa till med direktreglering behöver vi få', # if
    r'vi har mottagit en direktreglering gällande', # svedea
    r'för att vi ska kunna fortsätta handlägga skadeärendet behöver vi ytterligare komplettering', # svedea
    r'för vidare reglering av ärendet behöver vi ta del av', # lassie
    r'vi behöver nedanstående komplettering', # sveland
    r'inväntar komplettering från kunden för att kunna reglera', # dina
    r'här kommer önskemål om komplettering av en direktreglering',
    # Complement_Damage_Request_Insurance_Company # 6
    r'vi har tagit emot en (?:skadeanmälan|livskadeanmälan) från vår kund gällande [a-zåöä\d\-\.\s]*(?:undrar om ni kan|behöver veta)', # folksam
    r'för att kunna ta ställning i ersättningsfrågan.*?är vi tacksamma om', # if
    r'vi har fått in en ersättningsansökan från en kund som varit med sitt djur hos er\. vi skulle behöva få hjälp med nedanstående information från er', # sveland
    r'vi önskar ta del av fullständig journal', # svedea
    r'här kommer önskemål om komplettering av en skadeanmälan',
    r'märk gärna med:?.*?och FF\d+S\s',
  # Insurance_Validation_Error # 9
    r'manypets erbjuder inte längre djurförsäkring i sverige och vi direktreglerar inte längre skador',
    r' ingen[\wåöä ]+(?:försäkring|namn|person|pn) ',
    r'(?<= inte)[\wåöä ]+hitta[\wåöä ]+',
    r'hittar [\wåöä ]* ingen',
    r' saknas på angivet ',
    r'ingen gällande försäkring på behandlingsdatum',
    r'vi kan tyvärr inte finna någon försäkring här hos oss på kundens namn\/ adress',
    r'tyvärr ingen match med något av personnumret eller försäkringsnumret',
    r'försäkringsnumret är inte korrekt',
  # Settlement_Denied # 26
    r'dessvärre kan vi inte lämna ersättning för', 
    r'inte lämna någon ersättning i detta ärende',
    r'nekad ersättning pga', 
    r'(?:kan inte|saknar möjlighet att) direktreglera besök äldre än 7 dagar',
    r'tyvärr kan vi inte hjälpa till med direktreglering av följande skäl:', 
    r'inte kan ersätta denna skada',
    r'kan inte direktreglera detta (?:besök|underlag)',
    r'kommer därför inte lämna någon ersättning just nu',
    r'måste tyvärr meddela att de ersättningsbara kostnaderna för detta besök understiger den fasta självrisken i djurets försäkring',
    r'vår direktreglering gällande [a-zåöä]+ ser ut som följande: totalt fakturabelopp:[\d\s\-,]+kr nekad direktreglering, kund får inkomma med skadeanmälan',
    r'kan tyvärr inte genomföra direktregleringen på',
    r'inte direktreglera denna kostnad',
    r'kan vi tyvärr inte direktreglera',
    r'försäkringen kommer inte kunna lämna ersättning för detta besök',
    r'ingen ersättning av dagens kvitto',
    r'direktreglering gällande [^\W\d_\(\)\\\/\*\.\'"´, -]+ kan inte utföras',
    r'saknar möjlighet att direktreglera besök äldre än 7 dagar',
    r'\s0\s*kr ersättning',
    r'belopp att utbetala 0,00\s*kr',
    r'ersättning klinik:\s*0',
    r'ersättning: 0\s*kr',
    r'if betalar er 0\s*kronor',
    r'sveland betalar er 0\s*kronor',
    r'total ersättning som utbetalas: 0(?:\s*kr|,-|:-)',
    r'utbetalt belopp: 0\s*kr',
    r'vi nekar direktreglering gällande',
  # Settlement_Approved # 14
    r'- belopp att ersätta :', 
    r'belopp att utbetala', 
    r'bifogar ersättningsbrevet nedan för', 
    r'ersättning klinik:', 
    r'ersättning att betala ut :', # 5
    r'här kommer ersättningsbrevet', 
    r'här kommer sammanställningen', 
    r'här kommer svar på begärd direktreglering gällande', 
    r'total ersättning som utbetalas:', 
    r'utbetalt belopp:', # 10
    r'if betalar:?\s*[\d ,]+\s+kr', 
    r'vänligen notera försnr \d+', 
    r'vi (?:ersätter|ers) [\d\s]+(?:kr|:-)', 
    r'sveland betalar er', 
  # Complement_Reply', # 11
    r'du hittar patienthistoriken som en bifogad pdf fil',
    r'här kommer efterfrågad journal',
    r'här kommer efterfrågat kvitto',
    r'här kommer efterfrågat kvitto\/specifikation',
    r'här kommer efterfråga',
    r'här kommer fullständig journal',
    r'här kommer (?:journal(kopia)?|jk)',
    r'här kommer[\wåöä ]*(?:journal|kvitto|specifikation|journalkopia|jk|faktura)',
    r'här kommer svar på efterfrågad komplettering från kliniken',
    r'vänligen se bifogad journalkopia kliniken skickat angående nedanstående ärende',
    r'sätta 0 kr ersättning',
  # Settlement_Request # 5
    r'gällande förfrågan på djur:',
    r'ersätter [a-zåäö\s\/]+upp till försäkringens maxbelopp',
    r'ersätter ejkostnad för kastreringen',
    r'vi har mottagit ett förhandsbesked på',
    r'vi har tagit emot er förfrågan (?:gällande|angående)',
  # Message # 19
    r'avlivningskostnader ersätts inte\. men jag avslutar försäkringen',
    r'den här försäkringen är inte aktiv hos oss',
    r'det går bra att återbetala till',
    r'det här försäkringsnumret är inte i det formatet som våra försäkrinsnummer',
    r'det verkar som vi redan fått denna info',
    r'detta fnr tillhör ej oss och hittar endast en annan hund på angett pnr',
    r'detta svar har inte kommit in via api:et',
    r'e\-djur kan tyvärr inte hantera hästskador',
    r'jag hittade försäkringen med hjälp av den förra direktregleringen och hanterar den direkt',
    r'om du vill och kan vore det bra att få en lista på alla kliniker som ni har',
    r'se bilaga i denna mejltråd',
    r'vet inte om ni fått svar men direktreglering är nekad',
    r'vi har nu mottagit komplettering från kund och skickat beslutet om ersättning via direktregleringsportalen',
    r'vi har tyvärr inte kvittot\. kan det stå på mannen kanske',
    r'utbetald till fel klinik',
    r'betalningen är (?:nu )?stoppad och ni kan bortse från det ersättningsbesked som har skickats',
    r'har vidarebefordrat denna dir till svedea\. de kunder som tidigare haft ica-försäkring genom svedea ligger kvar hos dem och regleras av dem',
    r'klick här för att ange ett lösenord',
    r'kunden behöver godkänna detta innan vi kan gå vidare med direktregleringen',
  # Question' # 17
    r'\*vill kund att vi räknar med tidigare besök',
    r'\s*vi skulle vilja veta',
    r'\s+vill bara dubbelkolla med er att',
    r'\s+är det [^,.!;:]*\?',
    r'\s+hur[^,.!;:]*\?',
    r'\s+kan [^,.!;:]*\?',
    r'\s+när [^,.!;:]*\?',
    r'\s+ska [^,.!;:]*\?',
    r'\s+undrar om ni kan [^,.!;:]*\?',
    r'\s+vad [^,.!;:]*\?',
    r'\s+vem [^,.!;:]*\?',
    r'\s+vill ni [^,.!;:]*\?',
    r'\s+heter [^,.!;:]*\?',
    r'\s+(?:vilka|vilken) [^,.!;:]*\?',
    r'\s+har du möjlighet att [^,.!;:]*\?',
    r'\s+avser besöket något för [^,.!;:]*\?',
    r'\s+visst [^,.!;:]*\?',
  # Spam # 9
    r'transcript: america this week',
    r'\b(?:klubb|sex|älskare|träffas|hemlighet|dejting|impotent|lokal\sdating|bli\smedlem|coupon)\b',
    r'en ny plats på webben för diskreta överenskommelser',
    r'\so\strog',
    r'gratis prenumeration',
    r'socialt nätverk',
    r'vip[\-| ]kund',
    r'grupper[\wåöäÅÖÄ]* sex[\wåöäÅÖÄ]*',
    r' (?:kvinna|resebyråer) ',
     ]
regex_dict = {
    'category': category,
    'regex': regex}
# make_df(regex_dict, "category_reg")

#  ****************  2_2_3. Number Regex  ****************
number = [
  # 'date', # 2
    # 'date',
    # 'date',
  # 'settlementAmount', # 10
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount', # 5
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount', # 10
  # 'totalAmount', # 10
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount', # 5
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount', # 10
  # 'folksamOtherAmount', # 1
    'folksamOtherAmount',
  # 'reference', # 19 
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 5
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 10
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 15
    'reference',
    'reference',
    'reference',
    'reference',
  # 'insuranceNumber', # 12
    'insuranceNumber', 
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber', # 5
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber', # 10
    'insuranceNumber',
    'insuranceNumber',
  # 'damageNumber', # 6
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
  # 'animalName', # 15
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 5
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 10
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 15
  # 'animalName_Sveland', # 1
    'animalName_Sveland',
  # 'ownerName', # 8
    'ownerName',
    'ownerName',
    'ownerName',
    'ownerName',
    'ownerName', # 8
    'ownerName',
    'ownerName',
    'ownerName',
  # 'insuranCompany', # 1
    # 'insuranCompany',
    ]
regex = [
   # date #2
     # r'(?:Angående direktregleringen med datum|Besöksdatum:)\s([\d\-]+)', # date - folksam|sveland
     # r'Skickat: (\d{1,2} (januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december) \d{4})', # date - if 
   # settlementAmount # 10
    r'Total ersättning som utbetalas:\s+([\d ]+)', # Dina no,.
    r'(?:If betalar|Sveland betalar|Utbetalt belopp|Belopp att utbetala)\:?\s+([\d\, ]+)(?:,00)?', # If|Sveland|Lassie+Manypets|folksam no,.
    r'Ersättning att betala ut :\s+([\d\, ]+)(?:,00|kr)',
    r'Vi (?:ersätter|ers)\s?:? ([\d ]+)', # Sveland no,.
    r'Beslut för faktura #\d+\s*Regleringsnummer Försäkringsbolag Beslut[\d\s]+[\wåöäÅÖÄ ]+ ([\d,]+) kr', # DRP
    r'ERSÄTTNING KLINIK:?\s+([\d ]+)', # Agria no,.
    r'(?:Belopp att ersätta|Ersättning)\s?:?\s+([\d ]+)', # Sveland + Trygghansa no,.
    r'Ersättning ut till klinik:\s+([\d ]+)', # dunstan no,.
    r'(?:Betalt|betalar) till kliniken:\s+([\d ]+)', # hedvig no,.
    r'(?:Ersättningsbelopp|Svarad ersättning):\s*([\d ]+)', # trygghansa+sveland 127460 no,.
   # totalAmount # 10
    r'Totalt fakturabelopp:\s+([\d ]+)(?:,00)?', # Dina # Ersättningsbart: no,.
    r'Kvitton:\s+([\d ]+)', # if no,.
    r'Kvitto från \d{4}-\d{2}-\d{2}\s+([\d ]+)', # Agria no,.
    r'Veterinärvård\s+([\d ]+)(?:,00)?', # Folksam + TryggHansa# \*Ej ersättningsbart no,.
    r'Fakturabelopp\D+([\d ]+)', # If no,.
    r'Kvitto(?: [1|2] )?\D+([\d ]+) k', # Sveland no,.
    r'Summa att betala:?\s*([\d ]+)', # sveland no,.
    r'Skadespecifikation.*?:([\d ]+)', # Lassie no,.
    r'Skadekostnad: ([\d, ]+)', # Manypets no,.
    r'Totalsumma på anspråk:\s+([\d ]+)', # hedvig no,.
   # folksamOtherAmount, # 1
    r'Övrig kostnad\s+([\d\, ]+)(?:,00|kr)',
   # reference # 19
    r'\<mail\+(\d+)@drp\.se\>',
    r'Direktregleringsportalen.*?OCR\-nummer\s*(\d+)\s', # 219/Hedvig(18)+ica(4)+if(5)+Svedea(3)+sveland(20)+trygg(23)+wisentic(33)
    r'Beslut för faktura\s+#\d+\s+Regleringsnummer\s+Försäkringsbolag\s+Beslut\s+(\d+)',
    r'(?<=\[SUBJECT\]).*?(\d{10}).*?(?=\n\[BODY\])',
    r'(?:Direktregleringsnr\/journalnr):?.*?(\d+)',
    r'(?:Direktregleringsnr\/journalnr|Referensnummer:|Journalnummer|Er referens|Referens):?\s+(\d+)',# 434/folksam(27)+if(28)+sveland(15)+Lassie(111)+wicentic(202)
    r'(?:Er referens|Referens:)\s+#?(\d+)', # 967/folksam(963) lassie
    r'\s+#?(\d+)'
    r'Referens journal\/skadenummer (\d+)\/\d+\.', # 226/Wisentic(225)
    r'direktreglering (\d+) gällande.*?(?=\n\[BODY\])', # 6/provtcloud(1)+clinic(4)
    r'Skadereglering:\s+(\d+).*?(?=\n\[BODY\])', # 236/Dina(26)+Hedvig(24)+ica(3)+if(5)+Lassie(3)+trygghansa(24)+Moderna(6)+sveland(20)+svedea(2)+wicentis(59)
    r'Komplettering direktreglering\s+(\d+)', # 7/Sveland(2)
    r'Komplettering av direktreglering\s+FF\d+S\s+\((\d+)\)', # 2/clinic(2)+pc(1)
    r'ert referensnummer:\s*(\d+)', # svedea(2)
    r'(?:Försäkringsnummer|Djurförsäkring):\s+(\d{10})', # 8
    r'Betalningsreferens: \s+(\d+)', # trygghansa
    r'\(.*?(\d{10})\).*?(?=\n\[BODY\])', # 58/ica(2)+lassie(10)
    r'Ärende\s+(\d{10})', # 1/DRP
    r'DRP betalreferens\/ärendenummer:?\s*(\d+)',  # trygghansa
    r'Saknad betalning av ersättning:\s+(\d+)',  # trygghansa
   # insuranceNumber # 12
    r'Angående försäkringsfallsnr: (FF\d+S).*?(?=\n\[BODY\])', # 1204/folksam(1179)
    r'Försäkringsfallsnr (FF\d+S)', # folksam(1021)
    r'Direktregleringsportalen.*?Försäkringsnr:\s+([\w-]+)\s*Namn', # 194/dina(61)+hedvig(18)
    r'med försäkringsnummer: (\d+).*?(?=\n\[BODY\])', # 165/svedea(158)
    r'Märk med Komplettering av direktreglering (FF\d+S)', # 44/folksam(44)
    r'(?:Försäkringsnummer|Djurförsäkring):\s+([\w-]+)', # 196/Lassie(2)+Sveland(4)+if(54)+wisentic(131)
    r'((CV|cv|KD|kd)[\d-]+).*?(?=\n\[BODY\])', # insuranceNumber -5/Folksam(2)
    r'ingen[\wåöä ]+försäkring för .*? ([CVcvKDkd\d-]+)\s+på', # 2/folksam(2) 
    r'(?:förs[ \.]nr|försäkringsn|Försäkringsn|Försäkringn|försäkringn|försäkringsuppgift).*?(KD[\d-]+|CV[\d-]+|cv[\d-]+|[\d-]+).*?\[BODY\]', # insuranceNumber -182/Svedea(170)
    r'\[BODY\].*?(?:förs[ \.]nr|försäkringsn|Försäkringsn|Försäkringn|försäkringn|försäkringsuppgift)[^.?!\n]*?(KD[\d-]+|CV[\d-]+|cv[\d-]+|\d[\d-]+)', # insuranceNr_clinicComplementType -218/Wisentic(166)+Sveland(6)+svedea(9)+lassie(2)+if(2)+folksam(5)
    r'Försäkring:\s*([\w-]+)\s*Ägare:', # if
    r'Faktura är avböjd i handläggarreglering \- ([\d-]+) ', # Sveland
   # damageNumber # 6
    r'Referens journal\/skadenummer \d+\/([\d ]+)\.', # 225/wicentic(225)
    r'Skadenummer:?[ _]+?(([A-Za-z]+)?[0-9\- ]+)', # 42/lassie(31)+trygg(2)
    # r'DR ([A-Z\d\-]+)', # svedea(1)
    r'Vårt ärende:\s*([\d ]+)', # 58/if(47)
    r'Angående försäkringsfallsnr: (FF\d+S).*?(?=\n\[BODY\])', # 743/1204/folksam(1179)
    r'Skadespecifikation\s*(.*?)\s*\n',
    r'Faktura är avböjd i handläggarreglering \- ([\d-]+) ', # Sveland
   # animalName # 15
    r'via Direktregleringsportalen.*?(?:Djur|DJUR|Djurets).*?Namn:\s*([\p{L}\(\)\\\/\*\.\'"´, -]+)\n?.*?Djurart:',# 204/Dina(61)+Wisentic(30)+Trygg-Hansa(19)+Sveland(14)+Hedvig(15)
    r'Djurets namn:?\s*([\p{L}\(\)\\\/\*\.\'"´, -]+)(?= Referensnummer| Journalnummer|\n)', # 1028/folksam(1018)
    r'(?:Namn på djuret):?\s*([\p{L}\(\)\\\.\/\*\'"´, -]+)', # 198/sveland(3)+wicentic(225)
    r'(?:Namn|Djur|Djurets namn):.*?([\p{L}\(\)\\\.\/\*\'"´, -]+)', # 206/Folksam(123)+if(98)
    r' \- [\d-]+ ((?!kr ersättning)[\p{L}\(\)\\\/\*\.\'"´, -]+)(?=\n\[BODY\])', # 103/wicentic(101)
    r'gällande ([\p{L}\(\)\\\/\*\.\'"´, -]+) med.*?(?=\n\[BODY\])', # 136/svedea(136)
    r'gällande ([\p{L}\(\)\\\/\*\.\'"´, -]+).*?som står försäkrad på', # 117/svedea(117)
    r'(?:Komplettering )((?!av |direktreglering |\d+ |önskas gällande )[\p{L}\\\/\*\.\'"´, -]+).*?(?=\n\[BODY\])', # 43/lassie(33)
    r'(?:Direktreglering|Patienthistorik:) ([\p{L}\(\)\\\/\*\.\'"´-]+).*?(?=\n\[BODY\])', # 50/pc(20)+lassie(15)+sveland(3)+wisentic(2)+if(2)+folksam(1)
    r'önskas för ([\p{L}\(\)\\\/\*\.\'"´, -]+)(?: med försäkringsnummer).*?(?=\n\[BODY\])', # 8/svedea(10)
    r'(?<=\[SUBJECT\])([\p{L}\(\)\\\/\*\.\'"´,-]+) - [\d -]+(?=\n\[BODY\])', # 13/wicentic(13)
    r'ingen[\wåöä ]+försäkring för ([\p{L}]+) ', # 6/folksam(3)
    r'Vår direktreglering gällande ([\p{L}\(\)\\\/\*\.\'"´, -]+) ser ut som följande:', # dina
    r'Vi har fått in en direktreglering för ([\p{L}]+)', # ica
    r'Djurnamn:\s*(.*?)\n',
   # animalName_Sveland # 1
    r'(?<=\[SUBJECT\])([\p{L}\(\)\\\.\/\*\'"´,-]+)(?=\n\[BODY\])', # 65/sveland(36)
   # ownerName # 8
    r'Direktregleringsportalen.*?\*?Ägare\s*Namn:\s*(.*?)\s*Personnr', # 185/Dina(61)+Wisentic(30)+Trygg-Hansa(19)+Sveland(14)+Hedvig(15)
    r'Försäkringstagare\s+([\p{L}&, -]+)', # 548
    r'(?:Kund|Kundnamn):?\s+([\p{L}, -]+)\n', # 252/sveland(17)+wicentic(185)+pc(22)+if(6)+ica(4)+dina(1)+trygg(1) || 213
    r'(?<=Ägare: )([\p{L}, -]+)', # 202/folksam(153)+if(12) || 165
    r'som står försäkrad på ((?:(?! ert journalnr|_)[\p{L}, -]+))', # 143/svedea(137) || 136
    r'(?:Djurägarens namn:|Ägarens namn[:\.]?)\s+([\p{L}, -]+)', # 124/if(92) || 88
    r'ingen[\wåöä ]+försäkring.*? på (?:ägaren )?([A-ZÅÖÄ][\p{L} ]+?)(?= och|,| angivet| \d)', # 21/wisentic(5)+Folksam(5) || 4
    r'(?:Djurägare):\s+([A-ZÅÖÄ][\p{L}, -]+)', # 0
   # insuranCompany, # 1
    # r'Beslut för faktura #\d+\s*Regleringsnummer Försäkringsbolag Beslut[\d\s]+([\wåöäÅÖÄ ]+) \d', # DRP
    ]
nr_dict = {
    'number': number,
    'regex': regex }
# make_df(nr_dict, "numberReg")

number = [
  # 'date', # 2
    # 'date',
    # 'date',
  # 'settlementAmount', # 10
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount', # 5
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount',
    'settlementAmount', # 10
  # 'totalAmount', # 10
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount', # 5
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount',
    'totalAmount', # 10
  # 'folksamOtherAmount', # 1
    'folksamOtherAmount',
  # 'reference', # 18 
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 5
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 10
    'reference',
    'reference',
    'reference',
    'reference',
    'reference', # 15
    'reference',
    'reference',
    'reference',
  # 'insuranceNumber', # 12
    'insuranceNumber', 
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber', # 5
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber',
    'insuranceNumber', # 10
    'insuranceNumber',
    'insuranceNumber',
  # 'damageNumber', # 6
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
    'damageNumber',
  # 'animalName', # 15
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 5
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 10
    'animalName',
    'animalName',
    'animalName',
    'animalName',
    'animalName', # 15
  # 'animalName_Sveland', # 1
    'animalName_Sveland',
  # 'ownerName', # 8
    'ownerName',
    'ownerName',
    'ownerName',
    'ownerName',
    'ownerName', # 8
    'ownerName',
    'ownerName',
    'ownerName',
  # 'insuranCompany', # 1
    # 'insuranCompany',
    ]
regex = [
   # date #2
     # r'(?:Angående direktregleringen med datum|Besöksdatum:)\s([\d\-]+)', # date - folksam|sveland
     # r'Skickat: (\d{1,2} (januari|februari|mars|april|maj|juni|juli|augusti|september|oktober|november|december) \d{4})', # date - if 
   # settlementAmount # 10
    r'Total ersättning som utbetalas:\s+([\d ]+)', # Dina no,.
    r'(?:If betalar|Sveland betalar|Utbetalt belopp|Belopp att utbetala)\:?\s+([\d\, ]+)(?:,00)?', # If|Sveland|Lassie+Manypets|folksam no,.
    r'Ersättning att betala ut :\s+([\d\, ]+)(?:,00|kr)',
    r'Vi (?:ersätter|ers)\s?:? ([\d ]+)', # Sveland no,.
    r'Beslut för faktura #\d+\s*Regleringsnummer Försäkringsbolag Beslut[\d\s]+[\wåöäÅÖÄ ]+ ([\d,]+) kr', # DRP
    r'ERSÄTTNING KLINIK:?\s+([\d ]+)', # Agria no,.
    r'(?:Belopp att ersätta|Ersättning)\s?:?\s+([\d ]+)', # Sveland + Trygghansa no,.
    r'Ersättning ut till klinik:\s+([\d ]+)', # dunstan no,.
    r'(?:Betalt|betalar) till kliniken:\s+([\d ]+)', # hedvig no,.
    r'(?:Ersättningsbelopp|Svarad ersättning):\s*([\d ]+)', # trygghansa+sveland 127460 no,.
   # totalAmount # 10
    r'Totalt fakturabelopp:\s+([\d ]+)(?:,00)?', # Dina # Ersättningsbart: no,.
    r'Kvitton:\s+([\d ]+)', # if no,.
    r'Kvitto från \d{4}-\d{2}-\d{2}\s+([\d ]+)', # Agria no,.
    r'Veterinärvård\s+([\d ]+)(?:,00)?', # Folksam + TryggHansa# \*Ej ersättningsbart no,.
    r'Fakturabelopp\D+([\d ]+)', # If no,.
    r'Kvitto(?: [1|2] )?\D+([\d ]+) k', # Sveland no,.
    r'Summa att betala:?\s*([\d ]+)', # sveland no,.
    r'Skadespecifikation.*?:([\d ]+)', # Lassie no,.
    r'Skadekostnad: ([\d, ]+)', # Manypets no,.
    r'Totalsumma på anspråk:\s+([\d ]+)', # hedvig no,.
   # folksamOtherAmount, # 1
    r'Övrig kostnad\s+([\d\, ]+)(?:,00|kr)',
   # reference # 18
    r'Direktregleringsportalen.*?OCR\-nummer\s*(\d+)\s', # 219/Hedvig(18)+ica(4)+if(5)+Svedea(3)+sveland(20)+trygg(23)+wisentic(33)
    r'Beslut för faktura\s+#\d+\s+Regleringsnummer\s+Försäkringsbolag\s+Beslut\s+(\d+)',
    r'(?<=\[SUBJECT\]).*?(\d{10}).*?(?=\n\[BODY\])',
    r'(?:Direktregleringsnr\/journalnr):?.*?(\d+)',
    r'(?:Direktregleringsnr\/journalnr|Referensnummer:|Journalnummer|Er referens|Referens):?\s+(\d+)',# 434/folksam(27)+if(28)+sveland(15)+Lassie(111)+wicentic(202)
    r'(?:Er referens|Referens:)\s+#?(\d+)', # 967/folksam(963) lassie
    r'\s+#?(\d+)'
    r'Referens journal\/skadenummer (\d+)\/\d+\.', # 226/Wisentic(225)
    r'direktreglering (\d+) gällande.*?(?=\n\[BODY\])', # 6/provtcloud(1)+clinic(4)
    r'Skadereglering:\s+(\d+).*?(?=\n\[BODY\])', # 236/Dina(26)+Hedvig(24)+ica(3)+if(5)+Lassie(3)+trygghansa(24)+Moderna(6)+sveland(20)+svedea(2)+wicentis(59)
    r'Komplettering direktreglering\s+(\d+)', # 7/Sveland(2)
    r'Komplettering av direktreglering\s+FF\d+S\s+\((\d+)\)', # 2/clinic(2)+pc(1)
    r'ert referensnummer:\s*(\d+)', # svedea(2)
    r'(?:Försäkringsnummer|Djurförsäkring):\s+(\d{10})', # 8
    r'Betalningsreferens: \s+(\d+)', # trygghansa
    r'\(.*?(\d{10})\).*?(?=\n\[BODY\])', # 58/ica(2)+lassie(10)
    r'Ärende\s+(\d{10})', # 1/DRP
    r'DRP betalreferens\/ärendenummer:?\s*(\d+)',  # trygghansa
    r'Saknad betalning av ersättning:\s+(\d+)',  # trygghansa
   # insuranceNumber # 12
    r'Angående försäkringsfallsnr: (FF\d+S).*?(?=\n\[BODY\])', # 1204/folksam(1179)
    r'Försäkringsfallsnr (FF\d+S)', # folksam(1021)
    r'Direktregleringsportalen.*?Försäkringsnr:\s+([\w-]+)\s*Namn', # 194/dina(61)+hedvig(18)
    r'med försäkringsnummer: (\d+).*?(?=\n\[BODY\])', # 165/svedea(158)
    r'Märk med Komplettering av direktreglering (FF\d+S)', # 44/folksam(44)
    r'(?:Försäkringsnummer|Djurförsäkring):\s+([\w-]+)', # 196/Lassie(2)+Sveland(4)+if(54)+wisentic(131)
    r'((CV|cv|KD|kd)[\d-]+).*?(?=\n\[BODY\])', # insuranceNumber -5/Folksam(2)
    r'ingen[\wåöä ]+försäkring för .*? ([CVcvKDkd\d-]+)\s+på', # 2/folksam(2) 
    r'(?:förs[ \.]nr|försäkringsn|Försäkringsn|Försäkringn|försäkringn|försäkringsuppgift).*?(KD[\d-]+|CV[\d-]+|cv[\d-]+|[\d-]+).*?\[BODY\]', # insuranceNumber -182/Svedea(170)
    r'\[BODY\].*?(?:förs[ \.]nr|försäkringsn|Försäkringsn|Försäkringn|försäkringn|försäkringsuppgift)[^.?!\n]*?(KD[\d-]+|CV[\d-]+|cv[\d-]+|\d[\d-]+)', # insuranceNr_clinicComplementType -218/Wisentic(166)+Sveland(6)+svedea(9)+lassie(2)+if(2)+folksam(5)
    r'Försäkring:\s*([\w-]+)\s*Ägare:', # if
    r'Faktura är avböjd i handläggarreglering \- ([\d-]+) ', # Sveland
   # damageNumber # 6
    r'Referens journal\/skadenummer \d+\/([\d ]+)\.', # 225/wicentic(225)
    r'Skadenummer:?[ _]+?(([A-Za-z]+)?[0-9\- ]+)', # 42/lassie(31)+trygg(2)
    # r'DR ([A-Z\d\-]+)', # svedea(1)
    r'Vårt ärende:\s*([\d ]+)', # 58/if(47)
    r'Angående försäkringsfallsnr: (FF\d+S).*?(?=\n\[BODY\])', # 743/1204/folksam(1179)
    r'Skadespecifikation\s*(.*?)\s*\n',
    r'Faktura är avböjd i handläggarreglering \- ([\d-]+) ', # Sveland
   # animalName # 15
    r'via Direktregleringsportalen.*?(?:Djur|DJUR|Djurets).*?Namn:\s*([^\W\d_\(\)\\\/\*\.\'"´, -]+)\n?.*?Djurart:',# 204/Dina(61)+Wisentic(30)+Trygg-Hansa(19)+Sveland(14)+Hedvig(15)
    r'Djurets namn:?\s*([^\W\d_\(\)\\\/\*\.\'"´, -]+)(?= Referensnummer| Journalnummer|\n)', # 1028/folksam(1018)
    r'(?:Namn på djuret):?\s*([^\W\d_\(\)\\\.\/\*\'"´, -]+)', # 198/sveland(3)+wicentic(225)
    r'(?:Namn|Djur|Djurets namn):.*?([^\W\d_\(\)\\\.\/\*\'"´, -]+)', # 206/Folksam(123)+if(98)
    r' \- [\d-]+ ((?!kr ersättning)[^\W\d_\(\)\\\/\*\.\'"´, -]+)(?=\n\[BODY\])', # 103/wicentic(101)
    r'gällande ([^\W\d_\(\)\\\/\*\.\'"´, -]+) med.*?(?=\n\[BODY\])', # 136/svedea(136)
    r'gällande ([^\W\d_\(\)\\\/\*\.\'"´, -]+).*?som står försäkrad på', # 117/svedea(117)
    r'(?:Komplettering )((?!av |direktreglering |\d+ |önskas gällande )[^\W\d_\\\/\*\.\'"´, -]+).*?(?=\n\[BODY\])', # 43/lassie(33)
    r'(?:Direktreglering|Patienthistorik:) ([^\W\d_\(\)\\\/\*\.\'"´-]+).*?(?=\n\[BODY\])', # 50/pc(20)+lassie(15)+sveland(3)+wisentic(2)+if(2)+folksam(1)
    r'önskas för ([^\W\d_\(\)\\\/\*\.\'"´, -]+)(?: med försäkringsnummer).*?(?=\n\[BODY\])', # 8/svedea(10)
    r'(?<=\[SUBJECT\])([^\W\d_\(\)\\\/\*\.\'"´,-]+) - [\d -]+(?=\n\[BODY\])', # 13/wicentic(13)
    r'ingen[\wåöä ]+försäkring för ([^\W\d_]+) ', # 6/folksam(3)
    r'Vår direktreglering gällande ([^\W\d_\(\)\\\/\*\.\'"´, -]+) ser ut som följande:', # dina
    r'Vi har fått in en direktreglering för ([^\W\d_]+)', # ica
    r'Djurnamn:\s*(.*?)\n',
   # animalName_Sveland # 1
    r'(?<=\[SUBJECT\])([^\W\d_\(\)\\\.\/\*\'"´,-]+)(?=\n\[BODY\])', # 65/sveland(36)
   # ownerName # 8
    r'Direktregleringsportalen.*?\*?Ägare\s*Namn:\s*(.*?)\s*Personnr', # 185/Dina(61)+Wisentic(30)+Trygg-Hansa(19)+Sveland(14)+Hedvig(15)
    r'Försäkringstagare\s+([^\d_&, -][^\n]*)', # 548
    r'(?:Kund|Kundnamn):?\s+([^\W\d_, -]+)\n', # 252/sveland(17)+wicentic(185)+pc(22)+if(6)+ica(4)+dina(1)+trygg(1) || 213
    r'(?<=Ägare: )([^\W\d_, -]+)', # 202/folksam(153)+if(12) || 165
    r'som står försäkrad på ((?:(?! ert journalnr|_)[^\W\d_, -]+))', # 143/svedea(137) || 136
    r'(?:Djurägarens namn:|Ägarens namn[:\.]?)\s+([^\W\d_, -]+)', # 124/if(92) || 88
    r'ingen[\wåöä ]+försäkring.*? på (?:ägaren )?([A-ZÅÖÄ][^\W\d_ ]+?)(?= och|,| angivet| \d)', # 21/wisentic(5)+Folksam(5) || 4
    r'(?:Djurägare):\s+([A-ZÅÖÄ][^\W\d_, -]+)', # 0
   # insuranCompany, # 1
    # r'Beslut för faktura #\d+\s*Regleringsnummer Försäkringsbolag Beslut[\d\s]+([\wåöäÅÖÄ ]+) \d', # DRP
    ]
nr_dict = {
    'number': number,
    'regex': regex }
# make_df(nr_dict, "number_reg")

#  ****************  2_3_3. Attach Regex  ****************
number = [
    'attach_settlementAmount', # 5
    'attach_settlementAmount',
    'attach_settlementAmount',
    'attach_settlementAmount',
    'attach_settlementAmount',

    'attach_totalAmount', # 4
    'attach_totalAmount',
    'attach_totalAmount',
    'attach_totalAmount',
    
    'attch_reference', # 1
    
    'attch_insuranceNumber', # 2
    'attch_insuranceNumber',
    
    'attch_damageNumber', # 3
    'attch_damageNumber',
    'attch_damageNumber',
    'attch_damageNumber',
    
    'attch_animalName', # 3
    'attch_animalName',
    'attch_animalName',
    
    'attch_ownerName', # 3
    'attch_ownerName',
    'attch_ownerName',
    ]
regex = [
  # attach_settlementAmount # 5
    r'Utbetald ersättning.*?:?\s+([\d\. ]+) k?',  # svedea
    r'OBS!!.*?Vi betalar ut ([\d\. ]+) kr till er', # sveland
    r'BETALAR UT\s*([\d\. ]+) KR TILL ER',
    r'OBS!!!.*?Vi ersätter ([\d\. ]+)kr', # sveland
    r'Total ersättning\s+([\d ]+)', # sveland
  # attach_totalAmount # 4
    r'Kvitto.*?(?:\d{4}-\d{2}-\d{2})?\s+([\d\. ]+) k?', # svedea
    r'([\d ]+)\s*Totalt belopp\s*-+', # sveland
    r'Totalt belopp\s+([\d ]+)', # sveland
    r'Totalt belopp.*?([\d ]+)\n\d{4}-\d{2}-\d{2}', # sveland
  # attch_reference # 1
    r'Betalningsreferens\s+\-\s+(\d+)', # Svedea
  # attch_insuranceNumber # 2
    r'Försäkringsnummer:\s+(\d+)', # Svedea
    r'Försäkringsnr:.*?\n.*?\n.*?\n.*?\n.*?\n(.*?)\n', # Sveland
  # attch_damageNumber # 4
    r'Skadenummer:\s+([\w-]+\n\d+)\s', # Svedea
    r'Skadenummer:\s+([\w-]+)\n', # Svedea
    r'Skadenummer\s+(.*?)\n', # Svedea
    r'Skadenr:?.*?\n.*?\n.*?\n.*?\n.*?\n(.*?)\n', # Sveland
  # attch_animalName # 3
    r'Vi har fått din (?:skadeanmälan|direktreglering) för (.*?)\n?\. Här', # Svedea
    r'Vi har tagit emot en (?:skadeanmälan|direktreglering) för (.*?)\n?\s*och vi har ersatt kostnaden enligt följande beräkning', # Svedea
    r'Sammanställning av försäkringsersättning för (.*?)\n', # Sveland
  # attch_ownerlName # 3
    r'Sammanställning av försäkringsersättning för .*?\n(.*?)\n', # Sveland
    r'Skadenummer\s+.*\s+(.*)\n', # Svedea
    r'(^.*?)\n', # Svedea
    ]
att_dict = {
    'number': number,
    'regex': regex }
# make_df(att_dict, "attachReg")

#  ****************  3_1_1. Forward_Format  ****************
old = [
    r'\-[\-]+',
    '<br>###',
    '°°°<br>',
    r'<\/li><\/ul><br><br>',
    '§(<br>)+',
    r'"<b><i>Hej,<br>',
    r'<br>(<br>)+',
    r'\"<b><i>(<br>)*',
    r'(<br>)*</i></b>"',
    '§',
    '<br>Mvh'
    ]
new = [
    '',
    '',  
    '',
    '</li></ul>',
    '<br>',
    '<b><i>"Hej,<br><br>',
    '<br><br>',
    '<b><i>"',
    '"</i></b>',
    '',
    '<br><br>Mvh'
    ]
format_dict = {
               'oldText': old, 
               'newText': new}
# make_df(format_dict, "forwardFormat")

#  ****************  4_1_2. Payment_Info_Regex  ****************
item = [ 
    'sveland_extractDamageNumber',
    'sveland_extractOtherNumber',
    'sveland_extractReference',
    
    'agria_extractDamageNumber',
    'agria_extractOtherNumber',
    
    'dina_extractOtherNumber',
    
    'folksam_extractDamageNumber',
    'folksam_extractOtherNumber',
    
    'if_extractDamageNumber',
    'if_extractOtherNumber',
    
    'trygghansa_extractOtherNumber',
    ]
regex = [
    # Sveland
    r'(?:Skadenummer|Skade nummer):\s*(\d+)\s*\;', # DamageNumber
    r'(?:Skadenummer|Skade nummer):\s*\d+\s*;(?:F-)?(\d+)', # OtherNumber
    r'(?:Skadenummer|Skade nummer):\s*\d+\s*;?\d+\D+(\d+)', # Reference
    # Agria
    r'SKADENUMMER:\s*([\d\-]+)', # DamageNumber
    r'FAKTURANUMMER:.*?([\d-]+)',  # OtherNumber
    # Dina
    r'(\d+)', # OtherNumber
    # Folksam
    r'SKADENUMMER\n(FF\d+S)', # DamageNumber
    r'(?:FF\d+S)\n(.*?)\n', # OtherNumber
    # r'ATT UTBETALA\s*([\d\,]+ )KR',
    # If
    r'SKADENUMMER: (\d+)', # DamageNumber
    r'(?:FAKTURA|ERSÄTTNING ENL VÅRT BREV)\n*([\d+\-]+)', # OtherNumber
    # Trygghansa
     # r'(\d+)\n\*\*',
    r'\n#?(\d+)\n\*\*', # OtherNumber
     ]
regexDict = {
    'item': item,
    'regex': regex}
# make_df(regexDict, "infoReg")

#  ****************  4_2_2. Payment_Info_Bank_Name  ****************
bankName = [ 
    'Agria Djurförsäkring',
    'DINA FÖRSÄKRINGAR AB',
    'FOLKSAM ÖMSESIDIG SAKFÖRSÄKRING',
    'IF SKADEFÖRSÄKRING AB (PUBL)',
    'SVELAND DJURFÖRSÄKRINGAR ÖMSESIDIGT',
    'Trygg-Hansa Försäkring filial',]
reference = [
    'agria',
    'dina',
    'folksam',
    'if',
    'sveland',
    'trygghansa',
     ]
bankDict = {
    'bankName': bankName,
    'insuranceCompanyReference': reference}
# make_df(bankDict, "bankMap")

#  ****************  5_1_1. Model_List  ****************
modelDict = {'model': [
    'deepseek-r1-distill-llama-70b-specdec',
    # 'llama-3.3-70b-versatile',
    # 'llama-3.1-70b-specdec',
    # 'llama3-70b-8192',
    
    ]}
# make_df(modelDict, "model")

#  ****************  0_0. Action Suggestion  ****************
category = [
    "Auto_Reply", 
    "Settlement_Denied", 
    "Settlement_Approved", 
    "Information", 
    "Finance_Report", 
    "Spam", 
    "Wisentic_Error", 
    "Insurance_Validation_Error", 
    "Complement_DR_Insurance_Company", 
    "Complement_Damage_Request_Insurance_Company", 
    "Settlement_Request", 
    "Complement_DR_Clinic", 
    "Complement_Damage_Request_Clinic", 
    "Message", 
    "Question"]
actionSuggestion = [
    ["Connect", "Discard"],
    ["Connect", "Upgrade DR", "Discard"],
    ["Connect", "Upgrade DR", "Discard"],
    ["Discard"],
    ["Connect", "Download attachments", "Upload attachments", "Discard"],
    ["Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward / Resend", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"],
    ["Connect", "Forward", "Discard"]]
action_dict =  {
    "category": category,
    "actionSuggestion": actionSuggestion}
# make_df(action_dict, "actionSuggestion")

#### Run the following block to create 'regex' DF
# def create_chroma_db(label):
#     import chromadb
#     # import ollama
#     from tqdm import tqdm
#     from chromadb.utils import embedding_functions
    
#     client = chromadb.PersistentClient(path="./data/vector_db")
#     client.delete_collection(name="labeled_category")
#     sentence_transformer_emfn = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-mpnet-base-v2")
#     collection = client.create_collection(name="labeled_category", embedding_function=sentence_transformer_emfn)

#     for index, row in tqdm(label.iterrows(),total=len(label)):
#         # response = ollama.embeddings(model='mxbai-embed-large',prompt=row['key_sts'])
#         # embedding = response['embedding']
#         collection.add(
#                 ids=[str(index)],
#                 # embeddings=[embedding],
#                 metadatas=[{"category": row['category']}],
#                 documents=[row['key_sts']])
#     return None

# label = pd.read_csv("./data/labeled_category1.csv")
# create_vector_db(label)