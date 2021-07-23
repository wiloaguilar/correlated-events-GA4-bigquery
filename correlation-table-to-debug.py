#Import libraries 
  import pydata_google_auth
  from google.cloud import bigquery
  import pandas_gbq
  import datetime
  from datetime import datetime as dt
  import pandas as pd
  import time
  import seaborn as sns
  import matplotlib.pyplot as plt


#Bigquery Conexion 
  credentials = pydata_google_auth.get_user_credentials(
      ['https://www.googleapis.com/auth/cloud-platform'],
      # credentials_cache=pydata_google_auth.cache.REAUTH
      )
  PROJECT_ID = 'your id project' # you need change proyect-id by name's bigquery poyect where is the firebase data  
  bqclient = bigquery.Client(credentials=credentials, project=PROJECT_ID,)

#Function
def correlation_table (funnel:list,platform:str,table):
    fecha_cohorte = time.strftime("%Y-%m-%d")
    dateto = (dt.strptime(fecha_cohorte, "%Y-%m-%d") + datetime.timedelta(days=-7)).strftime('%Y%m%d')
    datefrom = (dt.strptime(fecha_cohorte, "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime('%Y%m%d') 
   
    sql = f"""
    SELECT event_date, event_name, COUNT(event_name) event_count
    FROM `{table}`
    WHERE event_date BETWEEN '{dateto}' AND '{datefrom}' AND platform = UPPER('{platform}')
    GROUP BY event_date, event_name
    ORDER BY event_date
    """
    df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID,credentials=credentials)
    df2 = (df.loc[df['event_name'].isin(funnel)])
    df3 = df2.astype({"event_date": int, "event_count":int, "event_name": str})
    df4 = df3.pivot_table('event_count',['event_date'],'event_name')
    df5 = df4.reindex(columns=funnel)

    correlation_mat = df5.corr()
    plt.figure(figsize=(10,5))
    sns.set(font_scale=1.4)
    sns.heatmap(correlation_mat, annot = True)

    varName=''
    variables = dict(globals())
    for name in variables:
      if variables[name] is funnel:
       varName = name
       break
    plt.title(varName,fontsize=22)
    return plt.show()

#Example
example_funnel =['name_event1', 'name_event2', 'name_event3']
correlation_table(example_funnel,'platform','name of flat table events from bigquery')
