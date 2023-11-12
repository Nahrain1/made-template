from sqlalchemy import create_engine
import pandas as pd
import sqlalchemy as sa

engine = create_engine("sqlite:///airports.sqlite")
meta = sa.MetaData()
table = sa.Table("airports", meta, 
                sa.Column("column_1", sa.Integer),
                sa.Column("column_2", sa.String),
                sa.Column("column_3", sa.String),
                sa.Column("column_4", sa.String),
                sa.Column("column_5", sa.String),
                sa.Column("column_6", sa.String),
                sa.Column('column_7', sa.Float),
                sa.Column('column_8', sa.Float),
                sa.Column('column_9', sa.Integer),
                sa.Column('column_10', sa.Float),
                sa.Column('column_11', sa.String),
                sa.Column('column_12', sa.String),
                sa.Column('geo_punkt', sa.String)
                )
meta.drop_all(bind=engine)
meta.create_all(bind=engine)
url = "https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv" 
df = pd.read_csv(url, delimiter=';')
df.to_sql(name='airports', con=engine, if_exists="replace", index=False)

