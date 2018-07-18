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
import loadingtransfers as ltr

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

#Setting dates for execution

first_date = dt.datetime(2018,05,11)
last_date = dt.datetime(2018,06,14)

logging.info('Running MSFT reports from %(first_date)s to %(last_date)s changes',{'first_date':str(first_date),'last_date':str(last_date)})
#Get transfers file for comparison

transfers=pd.DataFrame()
transfers=ltr.get_transfers_file_acc_642519()
transfers['transfid'] = pd.to_numeric(transfers['transfid'], errors='coerce')
transfers = transfers.reset_index(drop=True)



#Generate report according to the account and dates
monthlyreport=qp.createmonthlyreportforcolombiaacc642519(first_date=first_date,last_date=last_date)
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

#Locate Money transfer movements

locators = np.where(monthlyreport.operation_type == "MONEY_TRANSFER")[0]
locators.tolist()
locators=map(int, locators)


logging.info('Number of money transfers located= %(moneytransfers)s',{'moneytransfers':str(len(locators))})
for i in range(1,len(locators)):
    df = monthlyreport.loc[locators[i - 1]:locators[i]]
    df = df.reset_index(drop=True)
    df.drop(df.index[0], inplace=True)
    datefilename = df['operation_date'].iloc[-1]
    datefilename = datefilename[:10]
    datefilename = dt.datetime.strptime(datefilename, '%Y-%m-%d').strftime('%Y%m%d')

    transferid = pd.to_numeric(df['transaction_id'].iloc[-1])
    transfervalue = np.where(transfers.transfid == transferid)[0]
    transfervalue.tolist()
    transfervalue = map(int, transfervalue)
    if len(transfervalue)>0:
        valor = transfervalue[0]
        trm = transfers['trmrate'].loc[valor]
    else:
        valor = 0
    df2 = pd.to_numeric(df['net_amount'], errors='coerce')
    totalvalue = df2.sum()
    if totalvalue>1:
        filename = 'MSFTPAYU_CO_U1010CCOP01_' + str(datefilename)+ '_0' +'errorsuma'+('.xlsx')
        logging.info('File generated with possible errors. Please send to Development team for verification. File will be generated with name Filename= %(filename)s',
                     {'filename': str(filename)})
    else:
        filename = 'MSFTPAYU_CO_U1010CCOP01_' + str(datefilename) + '_0' + ('.xlsx')
        logging.info(
            'File has no issues. File will be generated with name Filename= %(filename)s',
            {'filename': str(filename)})
    batchname= 'MSFTPAYU_CO_U1010CCOP01_' + str(datefilename)+ '_0'
    df['batch_number_in_bank_deposit_file'] = str(batchname)
    df['batch_number_in_bank_deposit_file'] = str(batchname)
    df['reference'] = df['reference'].str.extract('(\d+)', expand=False)
    df['remmited_currency'] = str('USD')
    df['newindex'] = df.index + 1
    df['operation_amount_remitted_currency'] = str('=R') + df['newindex'].map(str) + ('*(1/$AC$2)')
    df['payu_fee_remitted_currency'] = str('=S') + df['newindex'].map(str) + ('*(1/$AC$2)')
    df['payu_fee_tax_remitted_currency'] = str('=T') + df['newindex'].map(str) + ('*(1/$AC$2)')
    df['net_amount_remmited_currency'] = str('=AB') + df['newindex'].map(str) + ('*(1/$AC$2)')
    df['exchange_rate'] = str("AC2")
    #df['exchange_rate'] = int(trm)
    df = df.drop('newindex', 1)
    writer = pd.ExcelWriter(filename)
    df.to_excel(writer, sheet_name='Hoja1', index=False, header=True)
    writer.save()
    valor = 0

logging.info('File generated with name Filename= %(filename)s. Send to NOC team for uploading at Microsoft FTP Service',{'filename': str(filename)})
logging.info('===============MSFT Reports Finished. For any information, please contact soporte@payulatam.com===============')