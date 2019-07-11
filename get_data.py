import pandas
import quandl
import pickle
import json
import datetime
import numpy as np
import pandas as pd
#from pandas.io import sql
import mysql.connector
import pymysql
import configparser
import sqlalchemy  as sql
#import MySQLdb

def store_last_date(storage,d,dt):
    if d not in storage.keys():
        storage[d]=dt
    elif dt > storage[d]:
        storage[d]=dt
    return


config = configparser.ConfigParser()
config.read_file(open('db.config'))


#outside loop
last_date_storage={}

# move this to config file
quandl.ApiConfig.api_key = 'AT9LRyJ4iQx8f29VQaR8'
f=open('datacode_industry.config')

# connect to db
cnx = pymysql.connect(user=config['mysqldb']['username'], \
	password=config['mysqldb']['password'],\
	host=config['mysqldb']['endpoint'],\
	database='industry', local_infile=True)

cursor = cnx.cursor()

lines = f.readlines()

count = 0
subsetlist = ['AAT','ABM','ACR','ACS','ADC','ADCX','ADE','ADY','ADYX',\
	'AEE','AEY','AFS','AGM','AOM','AROC','AROE','ASE','ATR']

for line in lines:
# inside loop
    dataset = line[:-1]
    print(dataset)
    for subset in subsetlist:
	datacode=dataset+'_'+subset
	#dataframe= quandl.get(datacode, start_date='2019-01-05', end_date='2019-07-05')
	dataframe= quandl.get(datacode)

	print(dataframe.index.max().date())
    	last_date = dataframe.index.max().date().strftime("%Y-%m-%d")
	store_last_date(last_date_storage, dataset, last_date)

    	print(last_date_storage)

    	dataframe.rename(index=str, columns={'Date':'dt',\
		'MCWA':'mcwa',\
		'80PCTL':'pctl_80',\
		'50PCTL':'pctl_50',\
		'20PCTL':'pctl_20',\
		'stocks':'n_stocks'}  )

    	dataframe.insert(0,'industry',dataset)
	dataframe.insert(0,'metric',subset)
	dataframe.to_csv('/home/ec2-user/work/datasets/'+datacode+'.csv')

	load_sql = "LOAD DATA LOCAL INFILE '/home/ec2-user/work/datasets/" + datacode + ".csv' INTO TABLE industry.industry_aggs FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
	print load_sql
	rows = cursor.execute(load_sql)

    count = count+1
    if count >2:
	cursor.execute('SELECT count(*) FROM industry.industry_aggs LIMIT 10;')
        x = cursor.fetchall()
	print x
	break

cnx.commit()
cnx.close()

with open('last_date_storage.json', 'w') as lds:
    json.dump(last_date_storage, lds)
