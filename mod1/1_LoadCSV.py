# Databricks notebook source
print ('Hello World')

# COMMAND ----------

import pandas as pd
url = 'https://www.stats.govt.nz/assets/Uploads/Business-price-indexes/Business-price-indexes-March-2019-quarter/Download-data/business-price-indexes-march-2019-quarter-csv.csv'
data = pd.read_csv(url)

# COMMAND ----------

data

# COMMAND ----------

type(data)

# COMMAND ----------

df=data
export_excel=df.to_excel(r'/dbfs/tmp/export_dataframeset.xlsx',index=None, header=True)
