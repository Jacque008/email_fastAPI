import os
import json
import pytz
import pandas as pd
from src.preprocess import PreProcess
from src.matchScenarios import MatchScenarios
from src.createForwarding import CreateForwarding
from src.paymentMatching import PaymentMatching
from src.llmSummary import LLMSummary
from src.chronologicalLog import ChronologicalLog
from src.utils import fetchFromDB
from flask import Flask,request,jsonify,render_template,abort,redirect,url_for,session
from flask_login import LoginManager,UserMixin,login_user,login_required,logout_user,current_user
from google.oauth2 import id_token
from datetime import datetime
from google.auth.transport import requests
from google_auth_oauthlib.flow import Flow
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

app.config['SECRET_KEY'] = os.getenv("SECRET_KEY") 
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

GOOGLE_CLIENT_ID = os.getenv('CLIENT_ID')
if os.getenv('ENV_MODE') == 'local':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    client_secrets_file = "other/pw/client_secret.json"  
    redirect_url='http://localhost:5000/login/callback'
elif os.getenv('ENV_MODE') in ['test', 'production']:
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '0'
    client_secrets_file = "/OAUTH2_CLIENT_SECRET_JIE/OAUTH2_CLIENT_SECRET"
    redirect_url='https://classify-emails-wisentic-596633500987.europe-west4.run.app/login/callback'
flow = Flow.from_client_secrets_file(
    client_secrets_file,
    scopes=['openid', 'https://www.googleapis.com/auth/userinfo.email'],
    redirect_uri=redirect_url)

class User(UserMixin):
    def __init__(self, id):
        self.id = id

@login_manager.user_loader
def load_user(user_id):
    return User(user_id) 

@app.route('/')  
def home():
    return render_template("login.html", userId = current_user.id if current_user.is_authenticated else None)

@app.route('/login')
def login():
    authorization_url, state = flow.authorization_url()
    session['state'] = state
    return redirect(authorization_url)

@app.route('/login/callback')
def callback():
    flow.fetch_token(authorization_response=request.url)
    credentials = flow.credentials
    id_info = id_token.verify_oauth2_token(credentials.id_token, requests.Request(), GOOGLE_CLIENT_ID)
    email = id_info.get('email')
    adminEmails = fetchFromDB(pp.adminQuery.format(COND="TRUE"))
    if email in adminEmails['email'].values:
        user = User(email)
        login_user(user)
        return redirect(url_for('dashboard'))
    else:
        return "Åtkomst nekad.", 403

@app.route('/dashboard')
@login_required
def dashboard():
    return render_template("dashboard.html", userId=current_user.id)

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('home'))

#################################
pp = PreProcess()
ms = MatchScenarios()
cf = CreateForwarding()
pm = PaymentMatching()
ls = LLMSummary()

@app.route('/category',methods=['GET','POST'])
@login_required
def category():
    result_dict = {}
    if request.method == 'POST':
        jsonList = request.files['emailJsonFile']
        if jsonList:
            emailJson = json.load(jsonList)
        
        # id = request.form.get('emailId') 
        # if id:
        #     emailJson = fetchFromDB(pp.emailSpecQuery.format(EMAILID=id))
                
            processed = pp.main(emailJson) 
            category = ms.main(processed)     
            result_dict = category.to_dict(orient='records')
            
    return render_template('category.html',record_json=result_dict) 

@app.route('/category_api',methods=['POST'])
def categoryApi():
    emailJson = request.json
    processed = pp.main(emailJson)
    category = ms.main(processed)
    result_json = category.to_json(orient='records')
    result_list = json.loads(result_json)
    return json.dumps(result_list)

@app.route('/forward',methods=['GET','POST'])
@login_required
def creatForwarding():
    result_dict = {}
    if request.method == 'POST':
      # test with file  
        # jsonFile = request.files.get('forwardEmailInJson')  
        # jsonList = json.load(jsonFile)
        # item = jsonList[0]
        # id = item['id']
        # correctedCategory = item['correctedCategory']
        # recipient = item['recipient']        
      # test with single ID  
        id = request.form.get('forwardEmailId') 
        if id:
            query = '''SELECT 
                            ecr."correctedCategory",
                            ecr."data" ->> 'recipient' AS "recipient"
                            FROM email_category_request ecr 
                            WHERE ecr."emailId"  = {EMAILID}''' 
            para = fetchFromDB(query.format(EMAILID=id))
            correctedCategory = para['correctedCategory'].iloc[0]
            recipient = para['recipient'].iloc[0]
            
        if correctedCategory in pp.forwCates:
            df = fetchFromDB(pp.emailSpecQuery.format(EMAILID=id)) 
            email = pp.main(df)
            
            email['correctedCategory'] = correctedCategory
            email['recipient'] = recipient
            email['textHtml'] = df['textHtml'].iloc[0]
            
            result = cf.main(email)
            
            result_dict = result.to_dict(orient='records')

        else:
            abort(400,description="Denna kategori kan inte vidarebefordras.")   
                
    return render_template('forward.html', record_json=result_dict)     
    
@app.route('/forward_api',methods=['POST'])
def creatForwardingApi():
    if not request.is_json:
        abort(400,description="Request must be in JSON format.")
        
    jsonList = request.json
    if not isinstance(jsonList,list) or len(jsonList) == 0:
        abort(400,description="Input JSON must be a non-empty list.")
        
    item = jsonList[0]
    
    required_keys = {'id','recipient','correctedCategory'}
    if not required_keys.issubset(item.keys()):
        abort(400,description=f"Missing required keys in the JSON item. Required keys: {required_keys}")
        
    if item['correctedCategory'] not in pp.forwCates:
        abort(400,description="This category cannot be forwarded.") 
        
    try:
        df = fetchFromDB(pp.emailSpecQuery.format(EMAILID=item['id']))
    except Exception as e:
        abort(500,description=f"Database fetch failed: {str(e)}")
        
    try:
        email = pp.main(df)
        email['correctedCategory'] = item['correctedCategory']
        email['recipient'] = item['recipient']
        email['textHtml'] = df['textHtml']
        result = cf.main(email)
        
    except Exception as e:
        abort(500,description=f"Email processing failed: {str(e)}")

    try:
        result_json = result.to_json(orient='records')
        result_list = json.loads(result_json)
        return json.dumps(result_list) 
    except Exception as e:
        abort(500,description=f"Failed to convert result to JSON: {str(e)}")
    
@app.route('/payment',methods=['GET','POST'])
@login_required
def paymentMatching():
    result_dict = {}
    if request.method == 'POST':
        
        jsonFile = request.files['paymentFileInJson']
        if jsonFile:
            jsonList = json.load(jsonFile)
            payDf = pd.DataFrame(jsonList)
        
        # id = request.form.get('paymentFileLineId') 
        # if id:
        #     payDf = fetchFromDB(pm.paymentQuery.format(ID=int(id)))
            
            result = pm.main(payDf)
            result_dict = result.to_dict(orient='records')
    return render_template('payment.html',record_json=result_dict)

@app.route('/payment_api',methods=['POST'])
def paymentMatchingApi():
    jsonList = request.json
    payDf = pd.DataFrame(jsonList)
    if pd.api.types.is_numeric_dtype(payDf['createdAt']):
        payDf['createdAt'] = pd.to_datetime(payDf['createdAt'], unit='ms')
    else:
        payDf['createdAt'] = pd.to_datetime(payDf['createdAt'])
        
    result = pm.main(payDf) 
    result['createdAt'] = result['createdAt'].astype(str).apply(lambda x: x[:19]) 
    result_json = result.to_json(orient='records')
    result_list = json.loads(result_json)

    return json.dumps(result_list) 

@app.route('/summary',methods=['GET','POST'])
@login_required
def summary():
    res = {
        "summaryClinic": None,
        "summaryIC": None,
        "summaryEmail": None,
        "summaryCommentDR": None,
        "summaryCommentEmail": None,
        "summaryCombine": None}
    
    errs = {
        "errorChat": None,
        "errorEmail": None,
        "errorCommentDR": None,
        "errorCommentEmail": None,
        "errorCombine": None}
    
    other = {
        "clinicName": None,
        "icName": None,
        "sender": None,
        "recipient": None}
    
    if request.method == 'POST':
        emailId = request.form.get('emailId', '').strip()
        reference = request.form.get('reference', '').strip()
        errandNumber = request.form.get('errandNumber', '').strip()
        
        inputs = [emailId, reference, errandNumber]
        activateInput = [value for value in inputs if value]  
        if len(activateInput) != 1:
            errorMessage = "Du måste ange ett av email-ID, referens eller ärendenummer, och endast ett får anges."
            for key in errs:
                errs[key] = errorMessage
            return render_template('summary.html', **res, **errs)
        
        condition = {}
        try:
            emailId = int(emailId) if emailId else None
        except ValueError:
            emailId = None
            
        if emailId:
            condition['chat'] = condition['email'] = f'er.id = (SELECT "errandId" FROM email WHERE id = {emailId})'                           
            condition['comment'] = f'(cr."emailId" = {int(emailId)} OR cr."errandId" = (SELECT "errandId" FROM email WHERE id = {emailId}))'
        elif reference:
            condition['chat'] = condition['email'] = condition['comment'] = f"ic.reference = '{reference}'"
        else:
            condition['chat'] = condition['email'] = condition['comment'] = f"er.reference = '{errandNumber}'"   
                
        (res["summaryClinic"], other["clinicName"], res["summaryIC"], other["icName"], errs["errorChat"], 
         res["summaryEmail"], other["sender"], other["recipient"], errs["errorEmail"],
         res["summaryCommentDR"], errs["errorCommentDR"], res["summaryCommentEmail"], errs["errorCommentEmail"],
         res["summaryCombine"], errs["errorCombine"]) = ls.main(condition, 'webService')
        
    return render_template('summary.html', **res, **errs, **other)

@app.route('/summary_api',methods=['GET','POST'])
def summaryAPI():    
    summaryCombine, errorCombine  = None, None

    if not request.is_json:
        abort(400, description="Reference must be in JSON format.")
 
    data = request.get_json()
    emailId = data[0].get('emailId', None)
    reference = data[0].get('reference', None)
    errandNumber = data[0].get('errandNumber', None)
    
    inputs = [emailId, reference, errandNumber]
    activateInput = [value for value in inputs if value]  
    if len(activateInput) != 1:
        abort(400, description="Du måste ange ett av email-ID, referens eller ärendenummer, och endast ett får anges.")
    
    condition = {}
    try:
        emailId = int(emailId) if emailId else None
    except ValueError:
        emailId = None
        
    if emailId:
        condition['chat'] = condition['email'] = f'er.id = (SELECT "errandId" FROM email WHERE id = {emailId})'                           
        condition['comment'] = f'(cr."emailId" = {int(emailId)} OR cr."errandId" = (SELECT "errandId" FROM email WHERE id = {emailId}))'
    elif reference:
        condition['chat'] = condition['email'] = condition['comment'] = f"ic.reference = '{reference}'"
    else:
        condition['chat'] = condition['email'] = condition['comment'] = f"er.reference = '{errandNumber}'"   
    
    summaryCombine, errorCombine = ls.main(condition, 'api')
                                     
    return jsonify({
        "Summary_Combined_Info": summaryCombine,
        "Error_Combined_info": errorCombine,
        })
 
@app.route('/log',methods=['GET','POST'])
@login_required
def log():
    groupLog, groupAI = None, None 
    if request.method == 'POST':
        start = request.form.get('startDate') 
        end = request.form.get('endDate') 
        errandNumber = request.form.get("errandNumber")
        if errandNumber and (start or end):
            return "Vänligen ange antingen ett datumintervall eller ett Ärende-ID, inte båda."
        elif not errandNumber and (not start and not end):
            return "Vänligen ange antingen ett datumintervall eller ett Ärende-ID."
        elif not errandNumber and (not start or not end):
            return "Vänligen ange både startdatum och slutdatum."
        
        if (start and end):
            cond1 = f"er.\"createdAt\" >= '{start} 00:00:00' AND er.\"createdAt\" < '{end} 00:00:00'"
            cond2 = True
        elif errandNumber:
            cond1 = True
            cond2 = f"er.reference = '{errandNumber}'"
            
        groqClient = ls._initialClient()    
        cl = ChronologicalLog(cond1, cond2, groqClient)
        groupLog, groupAI = cl.main()

    return render_template('log.html', groupLog=groupLog, groupAI=groupAI)# 

@app.route('/log_api',methods=['POST'])
def logAPI():    
    groupLog, groupAI, result = None, None, {}
    
    if not request.is_json:
        abort(400,description="Reference must be in JSON format.")

    jsonList = request.json
    if not isinstance(jsonList,list) or len(jsonList) == 0:
        abort(400,description="Input JSON must be a non-empty list.")
        
    errandNumber = jsonList[0]['errandNumber']
    cond1 = True
    cond2 = f"er.reference = '{errandNumber}'"
    groqClient = ls._initialClient()  
    cl = ChronologicalLog(cond1, cond2, groqClient)
    groupLog, groupAI = cl.main()
    
    for group_id, group in groupLog.items():
        result[group_id] = { 
            "Title": group["title"],
            "Chronological_Log": group["content"],
            "AI_Analysis": groupAI[group_id]
        }
                                
    return jsonify(result)

# SQL
    # @app.route('/sql',methods=['GET','POST'])
    # @login_required
    # def generateSQL():
    #     output = None
    #     if request.method == 'POST':
    #         input = request.form.get('input') 
    #         if not input:
    #             return "Vänligen ange dina krav här."
    #         else:
    #             sql = SqlGenerator(input)
    #             output = sql.main()
                
    #     return render_template('sql.html', output = output)
 
@app.route("/weekly-update", methods=["POST"])
def weekly_update():
    token = request.headers.get("X-Update-Secret")
    if not token:  
        abort(403, description="my own abort error.")

    clinic_old = pd.read_csv(f"{pp.folder}/clinic.csv")
    clinic_update = fetchFromDB(pp.updateClinicEmailQuery)
    
    old_group = clinic_old.sort_values(by=['clinicId', 'clinicEmail', 'clinicName']).groupby(['clinicId', 'clinicName'])
    keyword = pd.DataFrame()
    for _, group in old_group:
        colKeyword, colPC = [], [] 
        for _, row in group.iterrows():
            if pd.notna(row['keyword']) and row['keyword'] not in colKeyword:
                if ',' in row['keyword']:
                    keywords = [kw.strip() for kw in row['keyword'].split(',')]
                    colKeyword.extend([kw for kw in keywords if kw])
                else:
                    colKeyword.append(row['keyword'])  
            if pd.notna(row['provetCloud']) and row['provetCloud'] not in colPC:
                colPC.append(row['provetCloud']) 

        i = group.index[0]
        keyword.at[i, 'clinicId'] = group['clinicId'].iloc[0]
        keyword.at[i, 'keyword'] = ', '.join(colKeyword) if colKeyword else None
        keyword.at[i, 'provetCloud'] = ', '.join(colPC) if colPC else None
        
    clinic_temp = pd.merge(clinic_update, clinic_old[['clinicId','clinicEmail','keyword','provetCloud']], on=['clinicId','clinicEmail'], how='left').drop_duplicates()
    
    main = pd.merge(clinic_temp.loc[clinic_temp['role']=='main_email'], keyword, on=['clinicId'], how='left', suffixes=('_old', ''))
    main['keyword'] = main['keyword'].fillna(main['keyword_old'])
    main['provetCloud'] = main['provetCloud'].fillna(main['provetCloud_old'])
    main.drop(columns=['keyword_old', 'provetCloud_old'], inplace=True)
    remain = clinic_temp.loc[clinic_temp['role']!='main_email']
    clinic_new = pd.concat([main, remain], ignore_index=True).sort_values(by=['clinicId', 'clinicEmail', 'clinicName']).reset_index(drop=True)
    clinic_new.loc[:,'clinicName'] = clinic_new.loc[:,'clinicName'].str.strip()
    clinic_new.to_csv(f"{pp.folder}/clinic.csv", index=False)
    
    tz = pytz.timezone("Europe/Stockholm")
    update_time = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== ✅ Clinic email list updated successfully in {update_time} ====")
 
@app.route('/health')
def health_check():
    return jsonify(status="ok"), 200

@app.errorhandler(400)
def handle_400_error(e):
    return jsonify(error=str(e)),400  

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000) 
