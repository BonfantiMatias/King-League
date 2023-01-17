import pandas as pd
import time
import random
import os
from datetime import datetime

def get_data(url, liga):
    
    tiempo = [1,3,2]
    time.sleep(random.choice(tiempo))
    df = pd.read_html(url)
    df = df[14]
    df = pd.DataFrame(df)
    df = df[df.columns[1:9]]
    df = df.rename(columns={'Equipos.1':'Equipos'})
    df["Equipos"] = df["Equipos"].apply(lambda x: x.replace("*", "") if x.find("*") != -1 else x)
    df['LIGA'] = liga
    
    run_date = datetime_now()
    run_date = run_date.strtime("%Y-%m-%d")
    df['CREATED_AT'] = run_date
    
    return df



