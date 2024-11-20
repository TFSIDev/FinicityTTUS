import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd
import io
import requests
import json
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context
from requests.packages.urllib3.util.retry import Retry
import ssl
from urllib3.poolmanager import PoolManager
import importlib.util
import sys
import datetime


session = get_active_session()
customer_id = ""
start_time = ""
end_time = ""
mapping_df = ""
#mapping dict is the table related to the fund name
mapping_dict = ""
final = []
# mapping_df = session.table(fund_name).to_pandas()
# mapping_dict = mapping_df.to_dict(orient='records')

auth = {
    "prod": {
        "pId": "2445584500107",
        "secret": "sLxWwpDoe29lQzV6BSBE",
        "key": "136390eb2af10318686111a5758c047f"
    },
    "headers": {
        'Finicity-App-Key': '136390eb2af10318686111a5758c047f',
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Finicity-App-Token': 'AwardyN4OiUHD6oNJleQ'
    },
    "url": "https://api.finicity.com"
}
def prettify_name(name):
    return name.replace("_", " ").title()
    
@st.cache_data
def get_token():

    body = {
        "partnerId": auth["prod"]["pId"],
        "partnerSecret": auth["prod"]["secret"]
    }
    
    session = requests.Session()
    
    response = session.post(
    url=f"{auth['url']}/aggregation/v2/partners/authentication",
    json=body,
    headers=auth['headers'],
    # verify=cert_path
    )
    if response.status_code == 200:
        auth['headers']['Finicity-App-Token'] = response.json()['token']
        return auth['headers']['Finicity-App-Token']
    else:
        st.error(f"Failed to get token. Status code: {response.status_code}, Response: {response.text}")
        return None
        
if st.button("refresh token"):
    get_token()
    
def TransToExcel(input):
    transactions_df = pd.DataFrame(input)
    buffer = io.BytesIO()
    transactions_df.to_excel(buffer, index=False)
    buffer.seek(0)
    st.download_button(
        label="Download Transactions Report",
        data=buffer,
        file_name="transactions_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    st.success("Transactions report generated!")

#----------------------------------

def dateConverter(original_date_string):
    # Convert Unix timestamp to datetime object
    creationDate = datetime.datetime.fromtimestamp(int(original_date_string))

    # Format the datetime object
    formatted_date = creationDate.strftime('%m-%d-%Y')
    
    return formatted_date

#ForAllvue
@st.cache_data
def convertTransAllvue(arr, mapping_dict):
    # st.write(arr, "this is the arr", type(arr))
    # mapping_df[""]
    if len(arr) > 0:
        # all_keys = set().union(*(d.keys() for d in arr))
        keys_to_keep = {'amount', 'accountId', 'description', 'memo', 'transactionDate'}
        for index, j in enumerate(arr):
           
            # st.write(len(arr), "the length of transactions array")
            if len(arr) == 0:
                st.write("there are no transactions this timespan")
                return
            i = j.copy()
    
            categorization = i.pop("categorization")
            i.update(categorization)

            res = {p: i[p] for p in i if p in keys_to_keep}

            skip_values = ["Sweep Repo Interest", "SWEEP TO TREAS REPO I", "Sweep Repo Maturity"]
            if 'memo' in res and res['memo'] in skip_values:
                continue
            docNo = "000000"
            newNo = str(int(docNo) + index + 1).zfill(len(docNo))
            TableDict = {}
            
            for i in mapping_dict:
                # st.write("this is the ACCOUNT_ID",i["ACCOUNT_ID"], "and this is the res account_id", str(res["accountId"]))
                if i["ACCOUNT_ID"] == str(res["accountId"]):
                    TableDict = i
                    # st.write(TableDict, "THE TABLE DICT")

            res['Amount'] = res['amount']
            res["Amount (CCY)"] = res['amount']
            res["Amount (BCY)"] = res['amount']
            res['Amount (LCY)'] = res.pop('amount')
            res['Company Type'] = 'Fund'
            res["Security Description"] = ''
            res["Lot No."] = ''
            res["Due from gp Code"] = ''
            res["Due to master Code"] = ''
            res["SCY Code"] = ''
            res["BCY Code"] = ''
            res["ACY Code"] = ''
            aid = str(res["accountId"])
            # st.write("this is the tabler dict, " ,TableDict)
            accountCompanyCode = TableDict["ACCOUNTCOMPANYCODE"]
            BankNumber = TableDict["BANKNUMBER"]
            FundName = TableDict["FUND_NAME"]
            
            res['Company Code'] = accountCompanyCode

            res['Posting Date'] = dateConverter(str(res['transactionDate']))
            res['Document Date'] = dateConverter(str(res.pop('transactionDate')))
            # res['ActualSettleDate'] = dateConverter(str(res.pop('postedDate')))
            res['Document Type'] = ''
            res['Document No.'] = f'{res["Company Code"]}_Q2_{newNo}'
            res['External Document No.'] = ''
            res['Account Type'] = 'G/L Account'
            
            
            res['Bal. Account No.'] = BankNumber
            #name of the type of transaction
            # res['Account No.'] = accountBankLast4[FundName][aid]
            res['Account No.'] = '[INSERT GL/ACCOUNT HERE]'
            res.pop("accountId")
            #the code of the account
            # res['Account Name'] = fundStructure[FundName][aid]
            res['Account Name'] = ''
            description = res.pop('description', '')
            limited_desc = description[:250]
            res['Description'] = limited_desc
            res['Security No.'] = ''
            res['Amounts Relation Type'] = 'Exchange Rate'
            res['Quantity'] = ''
            res['Currency Code'] = 'USD'
            res['Exchange Rate Amount'] = '1.00'
            res['Relational Exch. Rate Amount'] = '1.00'
            res['Amount (SCY)'] = '0'
            res['CCY Code'] = 'USD'
            res['Exchange Rate Amount1'] = '1.00'
            res['Relational Exch. Rate Amount1'] = '1.00'
            res['Exchange Rate Amount2'] = ''
            res['Relational Exch. Rate Amount2'] = ''
            res['Exchange Rate Amount3'] = ''
            res['Relational Exch. Rate Amount3'] = ''
            res['Exchange Rate Amount4'] = ''
            res['Relational Exch. Rate Amount4'] = ''
            res['Amount (ACY)'] = '0.00'
            res['Bal. Account Type'] = 'Bank Account'
            
            res['Allocation Rule Code'] = 'INSERT ALLOCATION RULE HERE'
            res['Allocation Rule Description'] = ''
            res['Deal Code'] = ''
            res['Deal Description'] = ''
            memo = res.pop('memo', '')
            limited_memo = memo[:250]
            if 'memo' in res:
                res['Comment'] =limited_memo
            else: 
                res["Comment"] = ""
            res['Business Unit Code'] = ''
            final.append(res)
    # st.write(json.dumps(final), "the final result")
    else:
        st.write("no transactions this period")
        exit()
    # st.write(final[:50], "this is final")
    return final
#ForGenevaREC
@st.cache_data
def convertTransREC(arr, mapping_dict):
    all_keys = set().union(*(d.keys() for d in arr))
    keys_to_keep = {'amount', 'type', 'accountId', 'description', 'memo', 'postedDate', 'transactionDate', 'createdDate'}


    for j in arr:
        if len(arr) == 0:
            print("there are no transactions this timespan")
            exit()
        i = j.copy()
        categorization = i.pop("categorization")
        i.update(categorization)
        res = {p: i[p] for p in i if p in keys_to_keep}
        TableDict = {}
        # st.write( mapping_dict, "the mapping dict")
        
        for i in mapping_dict:
            if i["ACCOUNT_ID"] == str(res["accountId"]):
                TableDict = i
        if 'type' in res:
            if res['type'] == "debit":
                res['RecordType'] = 'Withdrawal'
            elif res['type'] == "credit":
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'cash':
                res['RecordType'] = 'Withdrawal'
            elif res['type'] == 'atm':
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'check':
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'deposit':
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'directDebit':
                res['RecordType'] = 'Sell'
            elif res['type'] == 'directDeposit':
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'dividend':
                res['RecordType'] = 'Dividend'
            elif res['type'] == 'fee':
                res['TradeExpenses.ExpenseAmt'] = abs(res['amount'])
            elif res['type'] == 'interest':
                res['RecordType'] = 'Interest'
            elif res['type'] == 'other':
                res['RecordType'] = '-'
            elif res['type'] == 'payment':
                res['RecordType'] = 'Withdrawal'
            elif res['type'] == 'pointOfSale':
                res['RecordType'] = 'Deposit'
            elif res['type'] == 'repeatPayment':
                res['RecordType'] = 'Repeat'
            elif res['type'] == 'serviceCharge':
                res['TradeExpenses.ExpenseAmt'] = abs(res['amount'])
            elif res['type'] == 'transfer':
                res['RecordType'] = 'Withdrawal'
        else:
            if res['amount'] < 0:
                res['RecordType'] = 'Sell'
            elif res['amount'] > 0:
                res['RecordType'] = 'Buy'
        res['NetInvestmentAmount'] = abs(res['amount'])
        if 'type' in res and res['type'] not in ('serviceCharge', 'fee'):
            res["Quantity"] = abs(res.pop('amount'))
        else:
            res["Quantity"] = 0
        res['RecordAction'] = 'InsertUpdate'
        res['KeyValue'] = 'NULL'
        accnt2 = res['accountId']
        res['Portfolio'] = TableDict["FUND_NAME"]
        res['FundStructure'] = TableDict["FUNDSTRUCTURE"]
        res['Strategy'] = "Undefined"
        res['EventDate'] = dateConverter(str(res['transactionDate']))
        res['SettleDate'] = dateConverter(str(res.pop('transactionDate')))
        res['ActualSettleDate'] = dateConverter(str(res.pop('postedDate')))
        res['BrokerName'] = 'UND'
        res['LocationAccount'] = TableDict["FUNDCODES"]
        res['Investment'] = 'USD'
        res['CounterInvestment'] = 'USD'  
        res['TradeExpenses.ExpenseNumber'] = 1.00
        res['TradeExpenses.ExpenseCode'] = 'Miscellaneous' 
        res['TotCommission'] = 0 
        res['NetCounterAmount'] = res['NetInvestmentAmount']
        res['tradeFX'] = 1
        res['PriceDenomination'] = 'USD'
        res['CounterFXDenomination'] = 'USD'
        res['Price'] = 1
        res.pop('accountId')
        res.pop('description')
        res["TradeExpenses.ExpenseAmt"] = 0
        if 'memo' in res:
            res.pop('memo')
        if 'type' in res:
            res.pop('type')
        final.append(res)
    # print(json.dumps(final), "the final result")
    return final

def getCustomerAccounts(customerId):
    get_token()
    response = requests.get(url=f"{auth['url']}/aggregation/v1/customers/{customerId}/accounts", headers=auth['headers'])
    json_data = json.loads(response.text)
    return json.dumps(json_data)

def generateConnectLink(customerID,partnerId):
    token = get_token()
    st.write(token)
    st.write(customerID)
    body = {
        "partnerId": partnerId,
        "customerId": customerID,
        "redirectUri": "https://www.finicity.com/connect/",
        "webhookContentType": "application/json",
        "webhookData": {},
        "webhookHeaders": {},
        "singleUseUrl": True,
        "institutionSettings": {},
        "fromDate": 1059756050,
        "experience" : "ae1b8ef6-9bf3-43f1-bbca-c15f2b82dbca",
        "reportCustomFields": [
            {
                "label": "loanID",
                "value": "123456",
                "shown": True
            }
        ]
    }
    response = requests.post(url = f"{auth['url']}/connect/v2/generate", headers=auth['headers'], json=body)
    json_data = response.json()
    st.write(json_data)
    link = json_data["link"]
    return link

def makeCustomer(body):
    token = get_token()
    auth['headers']['Finicity-App-Token'] = token
    response = requests.post(url=f"{auth['url']}/aggregation/v2/customers/active", json=body, headers=auth['headers'])
    if 200 <= response.status_code < 300:
        data = response.json()
        st.write(data)
        return data
    elif 400 <= response.status_code < 408:
        st.write(f"an error occurred: {response.status_code}")
    elif response.status_code ==409:
        st.write(f"Customer already exists!")
    elif 410 <= response.status_code <= 600:
        st.write(f"an error occurred: {response.status_code}")

# Define the taskbar
taskbar = st.sidebar.radio(
    "Navigation",
    ( "Reports", "Institutions", "Customers")
)

if taskbar == "Reports":
    st.title("Reports")
    
        # Query to fetch table names from the current database and schema
    query = "SHOW TABLES IN TESTINGAI.TESTINGAISCHEMA"
     
    # Execute the query and load table names into a DataFrame
    @st.cache_data(ttl=600)  # Cache the results for 10 minutes to avoid repeated queries
    def get_table_names():
        return pd.read_sql(query, st.connection('snowflake'))
     
    # Fetch the table names
    table_names_df = get_table_names()
     
    # Extract table names from the DataFrame
    table_names = table_names_df['name'].tolist()
     
    # Display a selectbox with the table names
    fund_name = st.selectbox("Fund Name", table_names)
     
    pretty_fund_name = prettify_name(fund_name)

    # Display the prettified fund name
    st.write(f"You selected: {pretty_fund_name}")
    try:
        mapping_df = session.table(fund_name).to_pandas()
        mapping_dict = mapping_df.to_dict(orient='records')
        # st.write(mapping_dict, "this is the mapping dict")
        if mapping_dict:
            customer_id = mapping_dict[0]["CUSTOMER_ID"]
            # st.write(customer_id, "this is the customerID")
            # first_entry_dict = mapping_dict[0]
            # customer_name = st.text_input("Customer Name",first_entry_dict['FUND_NAME'] )
            st.write('records found!')
            # st.write(,mapping_dict)
        
        else:
            st.write("No mapping found for the selected fund.")
    except Exception as e:
            st.error(f"Error fetching data: {e}")
    
    
    def human_to_unix(human_time):
        # Parse the human-readable time string into a datetime object
        dt_object = datetime.datetime.strptime(human_time, "%Y-%m-%d %H:%M:%S %Z")
        
        # Convert the datetime object to a Unix timestamp
        unix_timestamp = int(dt_object.timestamp())
        
        return unix_timestamp
        
        
    start_time = st.text_input("Start Time (IT MUST BE IN THIS FORMAT)", "2024-09-01 00:00:00 UTC")
    end_time = st.text_input("End Time (IT MUST BE IN THIS FORMAT)" , "2024-09-30 23:59:59 UTC")
    UnixStart = human_to_unix(start_time)
    UnixEnd = human_to_unix(end_time)

    database1 = st.selectbox("Database", ["Allvue", "Geneva"])
    if "Geneva" in database1:
         gen_report_type = st.selectbox("Geneva Report", ["REC", "ART"])
    # Transaction type input
    report_type = st.multiselect("Report Type", ["Statements", "Transactions"])
    
    @st.cache_data
    def getCustomerTrans(customerId, fromDate, toDate):
        get_token()
        params = {
            "fromDate": fromDate,
            "toDate": toDate,
            "limit": 1000,
            "includePending": True
        }
        response = requests.get(url=f"{auth['url']}/aggregation/v3/customers/{customerId}/transactions", headers=auth['headers'], params=params)
        st.write(f"{auth['url']}/aggregation/v3/customers/{customerId}/transactions")
        json_data = json.loads(response.text)
        return json_data
    # st.write("this is the trans for Parallaxes", getCustomerTrans(7031524383, UnixStart, UnixEnd))
    # st.write("this is the accounts for Parallaxes", getCustomerAccounts(7031524383))
    st.write("NOTE: It costs money each time you run a transaction or generate a statement. Please be conservative with how many requests you make! The date range and number of transactions do not matter, it is the frequncy of requests we are charged on.")
    if st.button("Generate Report"):
        if "Statements" in report_type:
            # Generate statements report (currently blank)
            statements_df = pd.DataFrame()  # Placeholder for actual data
            buffer = io.BytesIO()
            statements_df.to_excel(buffer, index=False)
            buffer.seek(0)
            st.download_button(
                label="Download Statements Report",
                data=buffer,
                file_name="statements_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            st.success("Statements report generated!")
        if "Transactions" in report_type:
            # Fetch transactions data
            # st.write(UnixStart, UnixEnd, customer_id)
            transactions = getCustomerTrans(customer_id, UnixStart, UnixEnd)
            st.write(transactions)
            transactions = transactions['transactions']
            if "Allvue" in database1:
                transactionsConv = convertTransAllvue(transactions, mapping_dict)
                TransToExcel(transactionsConv)
            if "Geneva" in database1:
                if "REC" in gen_report_type:
                    transactionsConv = convertTransREC(transactions, mapping_dict)
                    TransToExcel(transactionsConv)
                if "ART" in gen_report_type:
                    transactionsConv = convertTransART(transactions, mapping_dict)
                    TransToExcel(transactionsConv)

elif taskbar == "Institutions":
    st.title("Institutions")
    query = "SELECT * FROM TESTINGAI.INSTITUTIONS.INSTITUTIONS"
    instList = pd.read_sql(query, st.connection('snowflake'))
    st.write(instList)
    st.write("Can't find your institution? Search for it here:")
    
    @st.cache_data
    def getInstitutions(search):
        st.write(get_token())
        token = get_token()
        auth['headers']['Finicity-App-Token'] = token
        params = {
            "start": 1,
            "limit" : 1000,
            "search" : search
        }
    
        response = requests.get(url = f"{auth['url']}/institution/v2/institutions", headers=auth['headers'], params=params)
        data = response.json()
        return data
        
    search_term = st.text_input("type your bank name here")
    if st.button("Search Institution"):
        st.write(getInstitutions(search_term))
        
  
elif taskbar == "Customers":
    customer_ID = ""
    st.title("Add new customer")
    ClientName = st.text_input("Type client's name here. Ex: ExampleFundPartnersLLC. Do not use spaces.","ExampleFundPartnersLLC" )
    firstName = st.text_input("First name of the client or owner of fund. Ex: John", "John")
    lastName = st.text_input("Last name of the client or owner of fund. Ex: Jingleheimer", "Jingleheimer")


    customerBody = {
            "username": ClientName,
            "firstName": firstName,
            "lastName": lastName,
            "phone": "404-233-5275",
            "email": f"{ClientName}@tridenttrust.com",
            "applicationId" : '8407cf1e-b044-486f-a2bb-ed78cbfe4f16'
            }
            
    if st.button("Create Customer"):
        customer_data = makeCustomer(customerBody)
        if customer_data:
            customer_ID = customer_data["id"]
            st.write(customer_ID)
            
    if st.button("Generate Connect Link"):
        st.text_input("input the customer Id")
        connect_link_data = generateConnectLink(customer_id,auth["prod"]["pId"] )
        st.write(connect_link_data)