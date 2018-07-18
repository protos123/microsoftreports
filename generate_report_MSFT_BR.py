# -*- coding: utf-8 -*-
import sys
import os
import psycopg2 as db
import logging
import pandas as pd
import xlrd
import openpyxl
import datetime as dt
import numpy as np
import queries as qp
import slicing as sc
today = dt.datetime.today().strftime("%Y-%m-%d")
now=dt.datetime.now()

loggername='msftreports-'+str(today)+'.log'
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s', filename=loggername, filemode='a+',level=logging.DEBUG)
logging.info('================================MSFT Reports Executed================================')
logging.info('Executing MSFT reports at %(date)s. Starting database Connection',{'date':now})

# Intentar Conexion a BD
try:
    conn = db.connect(dbname='pol_v4', user='readonly', host='10.50.49.27', password='YdbLByGopWPS4zYi8PIR')
    cursor = conn.cursor()
except:
    logging.error('Cannot connect to database. Please run this script again')
    sys.exit()

#Setting monthly execution
#datenamefile=dt.datetime.today().strftime("%Y%m%d")
#first_date = dt.datetime(today.year, today.month, 1)
#last_date = dt.datetime(today.year, today.month + 1, 1)
# Define dates for start and end
first_date = dt.datetime(2018,06,18)
last_date = dt.datetime(2018,07,12)

#logging.info('Running MSFT reports from %(numbers)s changes',{'number)})


monthlyreport=qp.createmonthlyreportforbrazil(first_date=first_date,last_date=last_date)
monthlyreport.columns = ['operation_date','time_zone','account_id','merchant_account','operation_type','description',
                         'reference','transaction_id','order_id','batch_number_in_bank_deposit_file',
                         'currency_payment_request','amount_payment_request','payment_method','installments','promotion',
                         'authorization_code','operation_currency','operation_amount','payu_fee','payu_fee_tax',
                         'retentions','months_without_interest_fee','months_without_interest_tax','interest',
                         'interest_tax','chargeback_fee','chargeback_fee_tax','net_amount','exchange_rate',
                         'remmited_currency','operation_amount_remitted_currency','payu_fee_remitted_currency',
                         'payu_fee_tax_remitted_currency','months_without_interest_fee_remitted_currency',
                         'months_without_interest_tax_remitted_currency','interest_remitted_currency',
                         'interest_tax_remitted_currency','chargeback_fee_remitted_currency',
                         'chargeback_fee_tax_remitted_currency','net_amount_remmited_currency','account_balance',
                         'available_balance','sales_date']
# index=[]
# index= np.where(monthlyreport['operation_type']=='MONEY_TRANSFER')

#index=monthlyreport['operation_type'].values.tolist().index("MONEY_TRANSFER")
#print index

locators = np.where(monthlyreport.operation_type == "MONEY_TRANSFER")[0]
locators.tolist()
locators=map(int, locators)

for i in range(1,len(locators)):
    df=monthlyreport.loc[locators[i-1]:locators[i]]
    df=df.reset_index(drop=True)
    df.drop(df.index[0], inplace=True)
    datefilename= df['operation_date'].iloc[-1]
    datefilename = datefilename[:10]
    datefilename = dt.datetime.strptime(datefilename, '%Y-%m-%d').strftime('%Y%m%d')
    print datefilename
    df2=pd.to_numeric(df['net_amount'], errors='coerce')
    totalvalue=df2.sum()
    #print df2
    print totalvalue
    if totalvalue>1:
        filename = 'MSFTPAYU_BR_A1639EBRL01_' + str(datefilename)+ '_0' +'errorsuma'+('.xlsx')
    else:
        filename = 'MSFTPAYU_BR_A1639EBRL01_' + str(datefilename) + '_0' + ('.xlsx')
    batchname= 'MSFTPAYU_BR_A1639EBRL01_' + str(datefilename)
    df['batch_number_in_bank_deposit_file'] = str(batchname)
    df['reference'] = df['reference'].str.extract('(\d+)', expand=False)
    writer = pd.ExcelWriter(filename)
    df.to_excel(writer, sheet_name='Hoja1', index=False, header=True)
    writer.save()





#Executing primary report between dates in the month

