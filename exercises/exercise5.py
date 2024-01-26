import urllib.request
import zipfile
import pandas as pd
from sqlalchemy import create_engine

url = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
zip_location = 'GTFS.zip'

gtfs = urllib.request.urlretrieve(url, zip_location)

with zipfile.ZipFile(zip_location, 'r') as zip_ref: 
    zip_ref.extract("stops.txt")

stops_cols = ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]
df = pd.read_csv("stops.txt", usecols=stops_cols)
df = df[df['zone_id'] == 2001]

df = df.dropna(subset=['stop_name', 'stop_lat', 'stop_lon'])
df = df[(df['stop_lat'] >= -90) & (df['stop_lat'] <= 90)]
df = df[(df['stop_lon'] >= -90) & (df['stop_lon'] <= 90)]

engine = create_engine('sqlite:///gtfs.sqlite')
df.to_sql('stops', engine, if_exists='replace', index=False)
