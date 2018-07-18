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
today = dt.datetime.today()

def get_transfers_file_acc_642552():
    path= os.getcwd()
    files =os.listdir(path)
    files_xlsx=['Giros Internacionales.xlsx']
    logging.info('Data captured. Excel succesfully read')
    #Create dataframe and getting the date of the execution
    transfers=pd.DataFrame()
    #Bring file into dataframe
    for f in files_xlsx:
        data=pd.read_excel(f,'Comercios')
        transfers = transfers.append(data)
    transfers=transfers.rename(columns = {'Id Account':'accid','E-mail/ID Transfer':'transfid','Spot Rate':'trmrate'})
    transfers['accid'] = transfers['accid'].replace('null', np.nan).fillna(0)
    transfers['accid'] = pd.to_numeric(transfers['accid'], errors='coerce')
    trsflocat = np.where(transfers.accid == 642552)[0]
    trsflocat.tolist()
    trsflocat=map(int, trsflocat)
    msfttransfers=transfers.loc[trsflocat]
    return msfttransfers

def get_transfers_file_acc_642519():
    path= os.getcwd()
    files =os.listdir(path)
    files_xlsx=['Giros Internacionales.xlsx']
    logging.info('Data captured. Excel succesfully read')
    #Create dataframe and getting the date of the execution
    transfers=pd.DataFrame()
    #Bring file into dataframe
    for f in files_xlsx:
        data=pd.read_excel(f,'Comercios')
        transfers = transfers.append(data)
    transfers=transfers.rename(columns = {'Id Account':'accid','E-mail/ID Transfer':'transfid','Spot Rate':'trmrate'})
    transfers['accid'] = transfers['accid'].replace('null', np.nan).fillna(0)
    transfers['accid'] = pd.to_numeric(transfers['accid'], errors='coerce')
    trsflocat = np.where(transfers.accid == 642519)[0]
    trsflocat.tolist()
    trsflocat=map(int, trsflocat)
    msfttransfers=transfers.loc[trsflocat]
    return msfttransfers
