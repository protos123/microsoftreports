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


date='2018-04-03 00:03:28.742'
datenew=date[:10]
datenew=dt.datetime.strptime(datenew,'%Y-%m-%d').strftime('%Y-%m%d')
print datenew