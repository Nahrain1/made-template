import pytest
import os
import sqlite3
import sqlalchemy as sa
import pandas as pd
from pipeline import clean_dataset
import numpy as np
from sqlalchemy import create_engine, MetaData


@pytest.fixture
def pipeline_for_dataset1():
    url = "https://www.datenportal.bmbf.de/portal/Tabelle-0.64.csv"
    return clean_dataset(url=url, n=1)

@pytest.fixture
def pipeline_for_dataset2():
    url = "https://www.bka.de/SharedDocs/Downloads/DE/Publikationen/PolizeilicheKriminalstatistik/2022/Land/Tatverdaechtige/LA-TV-03-T40-Laender-TV-deutsch_csv.csv?__blob=publicationFile&v=4"
    return clean_dataset(url=url, n=2)

@pytest.fixture
def pipeline_for_dataset3():
    url = "https://www-genesis.destatis.de/genesis/downloads/00/tables/13211-0009_00.csv"
    return clean_dataset(url=url, n=3)

def test_check_df_instance (pipeline_for_dataset1, pipeline_for_dataset2, pipeline_for_dataset3):
    df1 = pipeline_for_dataset1
    df2 = pipeline_for_dataset2
    df3 = pipeline_for_dataset3
    assert isinstance(df1, pd.DataFrame)
    assert isinstance(df2, pd.DataFrame)
    assert isinstance(df3, pd.DataFrame)


def test_check_nulls (pipeline_for_dataset1, pipeline_for_dataset2, pipeline_for_dataset3):
    df1 = pipeline_for_dataset1
    df2 = pipeline_for_dataset2
    df3 = pipeline_for_dataset3
    assert df1.isnull().sum().sum() == 0
    assert df2.isnull().sum().sum() == 0
    assert df3.isnull().sum().sum() == 0


def test_table_existence():
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'mydata.db')
    connect = sqlite3.connect(data_path)
    cursor = connect.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    table_names = [table[0] for table in tables]
    assert "table1" in table_names
    assert "table2" in table_names
    assert "table3" in table_names
    connect.close()


def test_data_types_sample_col(pipeline_for_dataset1, pipeline_for_dataset2, pipeline_for_dataset3):
    df1 = pipeline_for_dataset1
    assert df1['2005'].apply(lambda x: isinstance(x, str)).all()
    df2 = pipeline_for_dataset2
    assert df2['4'].apply(lambda x: isinstance(x, str)).all()
    assert df2['6'].apply(lambda x: isinstance(x, str)).all()
    df3 = pipeline_for_dataset3
    assert df3['Years'].dtype == np.int64


def test_df_shape(pipeline_for_dataset1, pipeline_for_dataset2, pipeline_for_dataset3):
    df1 = pipeline_for_dataset1
    assert df1.shape == (18, 19)
    df2 = pipeline_for_dataset2
    assert df2.shape == (52, 24)
    df3 = pipeline_for_dataset3
    assert df3.shape == (35, 49)


