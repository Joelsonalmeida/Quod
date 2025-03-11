#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#credentials = service_account.Credentials.from_service_account_file(r"C:\Users\55119\Documents\JHOE\application_default_credentials.json")


# In[6]:


import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import glob
#import sed
from google.cloud import bigquery
from google.oauth2 import service_account

# Substitua pelo caminho do seu arquivo JSON
credentials = service_account.Credentials.from_service_account_file("C:/Users/55119/Documents/JHOE/fluent-optics-284222-a7285dcecd89.json")                                                                
client = bigquery.Client(credentials=credentials, project="fluent-optics-284222", location="US")


# In[7]:


# Caminho do arquivo no bucket
path = "gs://projeto-exemplo/QUOD/SAIDA/arquivo_convertido.parquet"


# In[8]:


# Carregando base no formato parquet
Base_Quod_Nova = pd.read_parquet(path)
print ('A base pussui {} linhas e {} colunas'.format(Base_Quod_Nova.shape[0],Base_Quod_Nova.shape[1]))
Base_Quod_Nova.head()


# In[9]:


#client = bigquery.Client(location="US")
client = bigquery.Client(project="fluent-optics-284222", location="US")
print("Client creating using dafault project: {}".format(client.project))

#Caminho que a base será enviada
table_id = 'fluent-optics-284222.projeto_exemplo.Base_Quod_Nova'

#Configuração de schema da query ao passar para o BQ
job_config = bigquery.LoadJobConfig(autodetect=True,write_disposition="WRITE_TRUNCATE")

job = client.load_table_from_dataframe(Base_Quod_Nova, table_id, job_config=job_config)

#Eseprar o retorno de envio concluido 
job.result()


# In[10]:


#client = bigquery.Client()

client = bigquery.Client(project="fluent-optics-284222") 

query = '''
CREATE OR REPLACE TABLE  `fluent-optics-284222.projeto_exemplo.Base_Quod` AS 
SELECT * FROM `fluent-optics-284222.projeto_exemplo.Base_Quod`
UNION ALL
SELECT * FROM `fluent-optics-284222.projeto_exemplo.Base_Quod_Nova`
Base_Quod_Nova;

'''
job = client.query(query)
job.to_dataframe()


# In[ ]:




