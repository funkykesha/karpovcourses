#!/usr/bin/env python
# coding: utf-8

# In[15]:


import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime
from datetime import date

from airflow import DAG
from airflow.operators.python import PythonOperator

import numpy as np


# In[2]:


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# In[27]:


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

def get_top_10_domain_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1])
    top_10_domain_zone = top_data_df.groupby('domain_zone', as_index=False)                                     .agg({'domain': 'count'})                                     .rename(columns={'domain': 'number'})                                     .sort_values('number', ascending=False)                                     .head(10)
    with open('top_10_domain_zone.csv', 'w') as f:
        f.write(top_10_domain_zone.to_csv(index=False, header=False))

def get_max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['len_name'] = top_data_df['domain'].apply(lambda x: len(x))
    max_len_domain = top_data_df[['rank', 'domain', 'len_name']].sort_values('len_name')                                                                 .tail(1)
    with open('max_len_domain.csv', 'w') as f:
        f.write(max_len_domain.to_csv(index=False, header=False))
        
def get_find_airflow():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    find = top_data_df.loc[top_data_df['domain'] == 'airflow.com']
    if find['domain'].count() == 0:
        rank_airflow = pd.DataFrame(np.array([['not_rank', 'airflow.com']]),
                                    columns=['rank', 'domain'])
    else:
        rank_airflow = find
        
    with open('rank_airflow.csv', 'w') as f:
        f.write(rank_airflow.to_csv(index=False, header=False))
        
def print_data(ds): # передаем глобальную переменную airflow
    with open('top_10_domain_zone.csv', 'r') as f:
        all_data_top_10_domain_zone = f.read()
    with open('max_len_domain.csv', 'r') as f:
        all_data_max_len_domain = f.read()
    with open('rank_airflow.csv', 'r') as f:
        all_data_find_airflow = f.read()
    date = ds

    print(f'Top 10 domains zone for date {date}')
    print(all_data_top_10_domain_zone)

    print(f'Max len of all domains for date {date}')
    print(all_data_max_len_domain)
    
    print(f'Rank of domain "airflow.com" for date {date}')
    print(all_data_find_airflow)


# In[28]:


default_args = {
    'owner': 'a.gaybadulin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 16),
    'schedule_interval': '0 6 * * *'
}
dag = DAG('dag_a_gaybadulin', default_args=default_args)


# In[29]:


t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_1 = PythonOperator(task_id='get_top_10_domain_zone',
                    python_callable=get_top_10_domain_zone,
                    dag=dag)

t2_2 = PythonOperator(task_id='get_max_len_domain',
                        python_callable=get_max_len_domain,
                        dag=dag)

t2_3 = PythonOperator(task_id='get_find_airflow',
                        python_callable=get_find_airflow,
                        dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)


# In[30]:


t1 >> [t2_1, t2_2, t2_3] >> t3

