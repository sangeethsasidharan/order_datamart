import sys
sys.path.append('/Workspace/Repos/sangeeth472@gmail.com/order_datamart/')
import pyspark
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from common_functions import *
from test_data_and_schema import *




spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

env_name = 'unit_test'

def compare_dataframe(df1, df2):
    df1_minus_df2 = df1.subtract(df2)
    df2_minus_df1 = df2.subtract(df1)

    if df1_minus_df2.count() == 0 and df2_minus_df1.count() == 0:
        return True
    else:
        print("DataFrames differ")
        df1_minus_df2.show()
        df2_minus_df1.show()
        return False    



def test_save_table():

    table_name = 'test_table'
    df = spark.createDataFrame(test_data, test_table_schema)
    save_table(spark, df , env_name, table_name)
    table_existes = spark.catalog.tableExists(f"{env_name}.{table_name}")
    assert table_existes is True


def test_read_table():
    table_name = 'test_table'
    df = read_table(spark, env_name, table_name)
    assert df.count() == 2

def test_format_column_names():

    products_df = spark.createDataFrame(raw_products_data, raw_products_schema)
    output_df = format_column_names(products_df)
    actual_column_name = output_df.columns
    assert format_column_expected_column_name == actual_column_name

def test_format_data():

    orders_df = spark.createDataFrame(enriched_data, enriched_orders_schema)
    expected_df = spark.createDataFrame(format_data_expected_data, format_data_expected_schema)
    output_df = format_data(orders_df, column_mapping['orders'])
    assert compare_dataframe(output_df, expected_df)

def test_drop_duplicates():

    df = spark.createDataFrame(drop_test_data, drop_test_table_schema)
    expected_df = spark.createDataFrame([(1, 'Ram'), (2, 'Shyam')], drop_test_table_schema)
    actual_df = drop_duplicates(df,'id', 'name')
    assert compare_dataframe(actual_df, expected_df)

def test_create_fact_table():

    products_df = spark.createDataFrame(fact_products_data, fact_products_schema)
    orders_df = spark.createDataFrame(fact_order_data, fact_orders_schema)
    customer_df = spark.createDataFrame(fact_customer_Data, fact_custmer_schema)
    expected_df = spark.createDataFrame(fact_expected_data, fact_expected_schema)
    actual_df = create_fact_table(products_df, orders_df, customer_df)
    assert compare_dataframe(actual_df, expected_df)

def test_create_agg_table():

    input_df = spark.createDataFrame(agg_fact_data, agg_fact_schema)
    agg_expected_df = spark.createDataFrame(agg_data_expected, agg_schema_expected)
    actual_df = create_agg_table(input_df)
    assert compare_dataframe(actual_df, agg_expected_df)

