import requests
import csv
from io import StringIO
from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy as sa
import os

""""""""""""""""""""" CREATE DATABASE 1  """""""""""""""""

url = "https://www.datenportal.bmbf.de/portal/Tabelle-0.64.csv"
response = requests.get(url)
if response.status_code == 200:
    
    db_path1 = os.path.join("C://Users//nahra//OneDrive//Desktop//Master//made-template//data","table1.sqlite")
    engine = create_engine(f"sqlite:///{db_path1}")

    meta = sa.MetaData()
    """table = sa.Table("table1", meta,
                  sa.Column("2005", sa.String),
                  sa.Column("2006", sa.String),
                  sa.Column("2007", sa.String),
                  sa.Column("2008", sa.String),
                  sa.Column("2009", sa.String),
                  sa.Column("2010", sa.String),
                  sa.Column("2011", sa.String),
                  sa.Column("2012", sa.String),
                  sa.Column("2013", sa.String),
                  sa.Column("2014", sa.String),
                  sa.Column("2015", sa.String),
                  sa.Column("2016", sa.String),
                  sa.Column("2017", sa.String),
                  sa.Column("2018", sa.String),
                  sa.Column("2019", sa.String),
                  sa.Column("2020", sa.String),
                  sa.Column("2021", sa.String),
                  sa.Column("2022", sa.String)
                  )"""
    meta.drop_all(bind=engine)
    meta.create_all(bind=engine)
      
    df = pd.read_csv(url, delimiter=';',skiprows=range(1, 5), nrows=18)
    # Set column names
    df.columns = ["year","2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014",
                  "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022"]

    df.to_sql(name='table1', con=engine, if_exists="replace", index=False)

else:
    print(f"Failed to fetch the CSV file. Status code: {response.status_code}")


""""""""""""""""""""" CREATE DATABASE 2  """""""""""""""""

url2 = "https://www.bka.de/SharedDocs/Downloads/DE/Publikationen/PolizeilicheKriminalstatistik/2022/Land/Tatverdaechtige/LA-TV-03-T40-Laender-TV-deutsch_csv.csv?__blob=publicationFile&v=4"

db_path2 = os.path.join("C://Users//nahra//OneDrive//Desktop//Master//made-template//data","table2.sqlite")
engine2 = create_engine(f"sqlite:///{db_path2}")
meta2 = sa.MetaData()
meta2.drop_all(bind=engine2)
meta2.create_all(bind=engine2)        
df = pd.read_csv(url2, delimiter=';',skiprows=range(53, 58091), nrows=53,encoding='latin1')
df.to_sql(name='table2', con=engine2, if_exists="replace", index=False)



