'''
function of this script:
	Daily: 
		1) Update sandbox_ana.bt_dailystock for BBT use.
		2) Refresh BT_DB on P drive for BBT.
    Monthly on the 1st:
    	1) everything in daily tasks
    	2) update sandbox_ana.cft_his_sales/ sandbox_ana.cft_his_stock/ sandbox_ana.bt_rangeplan
    	3) refresh CFT_DB on P drive.
	Send out an email with status
'''

import os, sys
# Append <project folder> & <src> to path
sys.path.append(os.path.abspath(os.path.join('..')))
sys.path.append(os.path.abspath(os.path.join('..', 'src')))
import pandas as pd
import numpy as np
import warnings
import win32com.client as win32
import time
import os
warnings.filterwarnings("ignore")
from connector import DatabaseRedshift
from datetime import datetime as dt

path = "P:\project00370\CFT Central\Data"
cft_DB = path + '\CFT_DB.xlsx'
bt_DB = path + '\BT_DB.xlsx'
recipients = ['hao.zhang@bestseller.com']


def read_sql(filename):

    fpath = os.path.abspath('..')+'\sql\\'
    fname = filename
    f = open(fpath+fname,'r')
    sql = f.read()
    f.close()
    return sql 


def get_nwc(df):

    #Calculation of forward looking net week cover(FNWC) and structure data. 

    df.net_cogs_ed = df.net_cogs_ed.astype('float64')
    df.eoh_val = df.eoh_val.astype('float64')
    df = df.sort_values(['noos','brand','product_category','fy','fq','cm'])
    df.reset_index(drop = True, inplace = True)
    df['cogs_weekly'] = df.net_cogs_ed / 4.3
    df["nwc"] = 0.0

    #FNWC calculation: for each month's EOH, calculate how many weeks' sales it can cover using next x weeks net COGS.
    #if go beyong data range we assume it continue consuming COGS using the last month available

    for i in range(0,len(df)-1):
        if df.product_category[i] == df.product_category[i+1]:
            j = 1
            while (i + j < len(df)-1)&(df.eoh_val[i] > df.net_cogs_ed[i+1:i+1+j].sum()) & (df.product_category[i] == df.product_category[i+j]):
                j = j + 1
            if i + j == len(df)-1:
                df.nwc[i] = 4.3 * (j-1) + (df.eoh_val[i] - df.net_cogs_ed[i+1:i+j].sum())/df.cogs_weekly[i+j]
            elif df.product_category[i] == df.product_category[i+j]:
                df.nwc[i] = 4.3 * (j-1) + (df.eoh_val[i] - df.net_cogs_ed[i+1:i+j].sum())/df.cogs_weekly[i+j]
            else:
                df.nwc[i] = 4.3 * (j-1) + (df.eoh_val[i] - df.net_cogs_ed[i+1:i+j].sum())/df.cogs_weekly[i+j-1]
        else:
            continue
    
    df.drop(['fq','cm','cogs_weekly'],axis=1,inplace=True)
    df.nwc.replace(to_replace=np.inf,value=999,inplace=True)
    return df

def RefreshExcel(filename):

    # Open Excel file and refresh all connections

    xl = win32.DispatchEx("Excel.Application")
    xl.visible = True
    wb = xl.workbooks.open(filename)
    wb.RefreshAll()
    time.sleep(30)
    wb.Save()
    xl.Quit()
    
def Mail(message,subject,recipients):

    # Send Email via Outlook

    outlook = win32.Dispatch('outlook.application')
    mail = outlook.CreateItem(0)

    if hasattr(recipients, 'strip'):
        recipients = [recipients]

    for recipient in recipients:
        mail.Recipients.Add(recipient)

    mail.Subject = subject
    mail.GetInspector
    
    index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body')) 
    mail.HTMLbody = mail.HTMLbody[:index + 1] \
                  + message                   \
                  + mail.HTMLbody[index + 1:] 
    
    mail.send    


# read sql scripts
sql_his_sales = read_sql('cft_his_sales.sql')

sql_his_stock = read_sql('cft_his_stock.sql')

sql_his_range = read_sql('bt_rangeplan.sql')

sql_dailystock = read_sql('bt_dailystock.sql')



log = []

# Test VPN connection 
print ('Establishing database connection...')
try:
    with DatabaseRedshift() as db:
        db.execute('select * from dwh.mart.fact_orderline limit 0',params={})
    log.append('Database connection established <br>')
except:
    Mail('Lost connecttion to Database <br>','Refresh Failed',recipients)
    exit()

# check if file exists

if os.path.exists(bt_DB) & os.path.exists(cft_DB):
    pass
elif os.path.exists(bt_DB):
    log.append(cft_DB + ' not found <br>')
elif os.path.exists(cft_DB):
    log.append(bt_DB + ' not found <br>')
else:
    log.append(bt_DB + ' not found <br>')
    log.append(cft_DB + ' not found <br>')

# refresh sandbox_ana.by_dailystock
print ('Updating sandbox_ana.bt_dailystock...')

try:    
    with DatabaseRedshift() as db:
        db.execute(sql_dailystock,params={})
    log.append('sandbox_ana.bt_dailystock refreshed <br>')
except:
    log.append('sandbox_ana.bt_dailystock refresh FAILED <br>')

#refresh buying tool database (BT_DB.xlsx)
print ('Refreshing BT_DB.xlsx...')

try:
    RefreshExcel(bt_DB)
    log.append('BT_DB refreshed <br>')
except:
    log.append(bt_DB + ' refreshed FAILED <br>')

#refresh CFT historical data monthly

if dt.now().day == 1:    
    print ('Updating sandbox_ana.cft_sales/sandbox_ana.bt_rangeplan...')

    try:
        with DatabaseRedshift() as db:
            db.execute(sql_his_sales,params={})
            print ('    cft_sales refreshed.')
            log.append('cft_sales refreshed <br>')
            db.execute(sql_his_range,params={})
            print ('    bt_rangeplan refreshed')
            log.append('bt_rangeplan refreshed <br>')
            print ('    fetching df_stock...')
            df_stock = db.fetch(sql_his_stock)
    except:
        log.append('his_sales/sql_his_range refreshed FAILED <br>')


    try:
        print ('Processing FNWC...')
        df_stock = get_nwc(df_stock)
        log.append('FNWC processed <br>')

        print ('Inserting df_stock...')
        with DatabaseRedshift() as db:
            db.insert(df=df_stock,table='cft_stock',schema='sandbox_ana',s3_csv_name='cft_stock.csv')
        log.append('cft_stock refreshed <br>')
    except:
        log.append('df_stock insert failed <br>')
    
    print ('Refreshing CFT_DB.xlsx')
    try:
        RefreshExcel(cft_DB)
        log.append('CFT_DB refreshed <br>')
    except:
        log.append(cft_DB + ' refresh FAILED <br>')

else:
    pass



if len(log) > 0:
    m = "".join(str(l) for l in log)
    Mail(m,'CFT refresh log',recipients)
else:
    m = "".join(str(l) for l in log)
    Mail(m,'Refresh Succeeded',recipients)