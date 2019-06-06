import pandas
import quandl
import pickle
import json
import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(style="darkgrid")


def store_last_date(storage,d,dt):
    if d not in storage.keys():
        storage[d]=dt
    elif dt > storage[d]:
        storage[d]=dt
    return





#outside loop
last_date_storage={}
quandl.ApiConfig.api_key = 'AT9LRyJ4iQx8f29VQaR8'

f=open('datacode.config')
lines = f.readlines()

count = 0

for line in lines:
# inside loop
    dataset = line[:-1]
    print(dataset)
    dataframe = quandl.get(dataset)
    print(dataframe.index.max().date())
    last_date = dataframe.index.max().date().strftime("%Y-%m-%d")
    store_last_date(last_date_storage, dataset, last_date)

    print(last_date_storage)

    print(dataframe)

    g = sns.relplot( kind="line", data=dataframe, y = dataset, x="date")
    g.fig.autofmt_xdate()

    plt.show()
    
    count = count+1
    if count >5:
        break

with open('last_date_storage.json', 'w') as lds:
    json.dump(last_date_storage, lds)
