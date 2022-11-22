#!/usr/bin/env python
# coding: utf-8

# In[20]:


import requests

from zipfile import ZipFile

from io import BytesIO
from io import StringIO


import pandas as pd
import numpy as np

from datetime import timedelta
from datetime import datetime
from datetime import date

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


# In[73]:


# задаем путь к файлу
path = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'


# In[113]:


# определяем год по формуле
my_year = 1994 + hash(f'a-gajbadulin-24') % 23


# In[45]:


default_args = {
    'owner': 'a-gajbadulin-24',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 21),
    'schedule_interval': '0 12 * * *'
    }


# In[74]:


# создаем функцию для уведомлений в Telegram
CHAT_ID = -67490173

try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''

def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass


# In[143]:


@dag(default_args=default_args, catchup=False)
def top_sales():
    # считываем данные, переводим имена столбцов в нижний регистр и фильтруем по году
    @task(retries=3)
    def get_data():
        data = pd.read_csv(path)
        data.columns = data.columns.str.lower()
        data = data.query('year == @my_year')

        return data
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries=3)
    def get_bestselling_game(get_data):
        bestselling_genre = data.groupby('genre', as_index=False).agg({'eu_sales': 'sum'})
        max_sales_genre = bestselling_genre['eu_sales'].max()
        bestselling_genre = bestselling_genre.query('eu_sales == @max_sales_genre')                                              .reset_index(drop=True)

        return bestselling_game.to_csv(index=False)
    
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries=3)
    def get_bestselling_genre(get_data):
        bestselling_platform_na = data.query('na_sales > 1')                                       .groupby('platform', as_index=False).agg({'name': 'count'})
        max_bestselling_platform_na = bestselling_platform_na['name'].max()
        bestselling_platform_na = bestselling_platform_na.query('name == @max_bestselling_platform_na')                                                          .reset_index(drop=True)
        
        return bestselling_genre.to_csv(index=False)
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task(retries=3)
    def get_bestselling_platform_na(get_data):
        meanselling_jp = data.groupby('publisher', as_index=False).agg({'jp_sales': 'mean'})
        max_meanselling_jp = meanselling_jp['jp_sales'].max()
        meanselling_jp = meanselling_jp.query('jp_sales == @max_meanselling_jp')                                        .reset_index(drop=True)
        
        return bestselling_platform_na.to_csv(index=False)
    
    # У какого издателя самые высокие средние продажи в Японии?
    # Перечислить все, если их несколько
    @task(retries=3)
    def get_meanselling_jp(get_data):
        meanselling_jp = data.groupby('publisher', as_index=False).agg({'jp_sales': 'mean'})                              .reset_index(drop=True)
        max_meanselling_jp = meanselling_jp['jp_sales'].max()
        meanselling_jp = meanselling_jp.query('jp_sales == @meanseller_jp')

        return meanselling_jp.to_csv(index=False)

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries=3)
    def get_count_eu_more_jp(get_data):
        eu_more_jp = data[['name', 'eu_sales', 'jp_sales']].copy()
        eu_more_jp['eu_more_jp'] = eu_more_jp['eu_sales'] > eu_more_jp['jp_sales']
        count_eu_more_jp = eu_more_jp.loc[eu_more_jp['eu_more_jp'] == True].name.count()

        return count_eu_more_jp.to_txt(index=False)
    
    @task(on_success_callback=send_message)
    def print_data(bestselling_game, 
                   bestselling_genre, 
                   bestselling_platform_na, 
                   meanselling_jp, 
                   count_eu_more_jp):

        context = get_current_context()
        date = context['ds']
        
        # Для bestselling_game
        result = bestselling_game
        if result['name'][0] == result['name'][len(result['name'])-1]:
            a = 'Best selling game of ' + str(my_year) + 'is ' + '"' + result['name'][0] + '".'
        else:
            a = 'Best selling games of ' + str(my_year) + ' are '
            for i in result.index:
                if result['name'][i] == result['name'][0]:
                    a += '' + '"' + result['name'][i] + '"'
                elif result['name'][i] == result['name'][len(result['name'])-1]:
                    a += ' and ' + '"' + result['name'][i] + '".'
                else:
                    a += ', ' + '"' + result['name'][i] + '"'
        print(a)

        # Для bestselling_genre
        result = bestselling_genre
        if result['genre'][0] == result['genre'][len(result['genre'])-1]:
            a = 'Best selling genre of ' + str(my_year) + ' in Europe is ' + '"' + result['genre'][0] + '".'
        else:
            a = 'Best selling genres in ' + str(my_year) + ' in Europe are '
            for i in result.index:
                if result['genre'][i] == result['genre'][0]:
                    a += '' + '"' + result['genre'][i] + '"'
                elif result['genre'][i] == result['genre'][len(result['genre'])-1]:
                    a += ' and ' + '"' + result['genre'][i] + '".'
                else:
                    a += ', ' + '"' + result['genre'][i] + '"'
        print(a)

        # Для bestselling_platform_na
        result = bestselling_platform_na
        if result['platform'][0] == result['platform'][len(result['platform'])-1]:
            a = 'Best selling platform of ' + str(my_year) + ' in Northen America is '                 + '"' + result['platform'][0] + '".'
        else:
            a = 'Best selling platforms in ' + str(my_year) + ' in Northen America are '
            for i in result.index:
                if result['platform'][i] == result['platform'][0]:
                    a += '' + '"' + result['platform'][i] + '"'
                elif result['name'][i] == result['platform'][len(result['platform'])-1]:
                    a += ' and ' + '"' + result['platform'][i] + '".'
                else:
                    a += ', ' + '"' + result['platform'][i] + '"'
        print(a)

        # Для meanselling_jp
        result = meanselling_jp
        if result['publisher'][0] == result['publisher'][len(result['publisher'])-1]:
            a = 'Publisher with highest average bill of ' + str(my_year) + ' in Japan is '                 + '"' + result['publisher'][0] + '".'
        else:
            a = 'Publishers with highest average bill in ' + str(my_year) + ' in Japan are '
            for i in result.index:
                if result['publisher'][i] == result['publisher'][0]:
                    a += '' + '"' + str(result['publisher'][i]) + '"'
                elif result['publisher'][i] == result['publisher'][len(result['publisher'])-1]:
                    a += ' and ' + '"' + result['publisher'][i] + '".'
                else:
                    a += ', ' + '"' + result['publisher'][i] + '"'
        print(a)

        # Для count_eu_more_jp
        result = meanselling_jp
        a = 'The number of games that sold better in Europe than in Japan of '             + str(my_year) + ' is ' + str(count_eu_more_jp) + '.'
        print(a)
        
    
    data = get_data()
    
    bestselling_game = get_bestselling_game(get_data)
    bestselling_genre = get_bestselling_genre(get_data)
    bestselling_platform_na = get_bestselling_platform_na(get_data)
    meanselling_jp = get_meanselling_jp(get_data)
    count_eu_more_jp = get_count_eu_more_jp(get_data)
    
    print_data(bestselling_game, 
               bestselling_genre, 
               bestselling_platform_na, 
               meanselling_jp, 
               count_eu_more_jp)
    
top_sales = top_sales()

