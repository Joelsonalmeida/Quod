#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import xml.etree.ElementTree as ET
import gcsfs as gcsfs


# In[17]:


# Caminho do arquivo no GCS
xml_file = 'gs://projeto-exemplo/QUOD/ENTRADA/arquivo_convertido.xml'

# Criar um sistema de arquivos GCS e abrir o arquivo
fs = gcsfs.GCSFileSystem()

with fs.open(xml_file, 'rb') as f:
    tree = ET.parse(f)

root = tree.getroot()

data = []

# Extrair dados do XML
envo = {
    'CnpjIf': root.findtext('CnpjIf'),
    'CnpjGbd': root.findtext('CnpjGbd'),
    'NrRms': root.findtext('NrRms'),
    'SeqlRms': root.findtext('SeqlRms'),
    'DtRms': root.findtext('DtRms'),
    'TipCli': root.find('Cli/TipCli').text,
    'IdfcCli': root.find('Cli/IdfcCli').text,
    'NmCli': root.find('Cli/NmCli').text,
    'NrUnco': root.find('Cli/Opr/NrUnco').text,
    'PrfAg': root.find('Cli/Opr/PrfAg').text,
    'NrCtr': root.find('Cli/Opr/NrCtr').text,
    'DtCtrc': root.find('Cli/Opr/DtCtrc').text
}
data.append(envo)

# Criar DataFrame
df = pd.DataFrame(data)

# Salvar como Parquet no GCS
parquet_file = 'gs://projeto-exemplo/QUOD/SAIDA/arquivo_convertido.parquet'
df.to_parquet(parquet_file, engine='pyarrow', storage_options={'token': None})

print(f'Arquivo Parquet salvo em: {parquet_file}')


# In[ ]:





# In[ ]:





# In[ ]:




