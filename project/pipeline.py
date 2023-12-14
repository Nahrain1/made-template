from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy as sa
import os
import numpy as np 

def clean_dataset(url, n):

    #database_path = os.path.join("C:\\Users\\nahra\\OneDrive\\Desktop\\Master\\made-template\\data",f"table{n}.sqlite")
    #engine = create_engine(f"sqlite:///{database_path}")
    engine = sa.create_engine('sqlite:///:memory:')
    meta = sa.MetaData()
    meta.drop_all(bind=engine)
    meta.create_all(bind=engine)
      
    if n == 1: 
        df = pd.read_csv(url, delimiter=';',skiprows=range(1, 5), nrows=18)
        # Set column names
        df.columns = ["year","2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014",
                    "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]
        replacements = {'c': '', '(': '', ')': ''}
        df = df.replace(to_replace=replacements)
        df = df.replace(to_replace=r'\((\d+)\)', value=r'\1', regex=True)
        df.to_sql(name='table1', con=engine, if_exists="replace", index=False)  
       
    elif n==2:
        df = pd.read_csv(url, delimiter=';',skiprows=range(53, 58091), nrows=53,encoding='latin1')
        df = df.drop(df.columns[0], axis=1)
        df.to_sql(name='table2', con=engine, if_exists="replace", index=False)
      
    else: 
        df = pd.read_csv(url, delimiter=';',skiprows=range(0, 3), nrows=35,encoding='latin1')
        # Rename the "Unnamed: 0" column to Years
        df.rename(columns={"Unnamed: 0": "Years"}, inplace=True)
        df.fillna(0, inplace=True)
        df['Years'] = df['Years'].astype(int)
        df.to_sql(name='table3', con=engine, if_exists="replace", index=False)
        
    return df
            
url1 = "https://www.datenportal.bmbf.de/portal/Tabelle-0.64.csv"
df1 =clean_dataset(url1, 1)


url2 = "https://www.bka.de/SharedDocs/Downloads/DE/Publikationen/PolizeilicheKriminalstatistik/2022/Land/Tatverdaechtige/LA-TV-03-T40-Laender-TV-deutsch_csv.csv?__blob=publicationFile&v=4"
df2 = clean_dataset(url2, 2)

url3 = "https://www-genesis.destatis.de/genesis/downloads/00/tables/13211-0009_00.csv"
df3 = clean_dataset(url3, 3)