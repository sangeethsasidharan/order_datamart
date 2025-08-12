from pyspark.sql import functions as F
from pyspark.sql import Window

def format_column_names(df):
    """
    Input : df which need to be formatted
    Output : df with correct column names
    Purpose: To replce '-' and space from column name with _
    """
  return df.select([F.col(column).alias(column.lower().replace('-', '_').replace(' ', '_')) for column in df.columns])

def save_table(spark, df, env_name , table_name):
  spark.sql(f"create schema IF NOT EXISTS {env_name}")
  spark.sql(f"drop table if exists {env_name}.{table_name}")
  df.write.format("delta").mode("overwrite").saveAsTable(f"{env_name}.{table_name}")

def read_table(spark, env_name , table_name):
  return spark.read.table(f"{env_name}.{table_name}")

# to map the data types to targeted types
column_mapping = {
    'product': {
        'product_id': 'string',
        'category': 'string',
        'sub_category': 'string',
        'product_name': 'string',
        'state': 'string',
        'price_per_product': 'double'
    },
    'orders': {
        'customer_id': 'string',
        'discount': 'double',
        'order_date': ['date', 'd/M/yyyy'],
        'order_id':  'string',
        'price': 'double',
        'product_id': 'string',
        'profit' : 'double',
        'quantity' : 'long',
        'row_id': 'long',
        'ship_date': ['date', 'd/M/yyyy'],
        'ship_mode': 'string'
    },
    'customer': {
        'customer_id': 'string',
        'customer_name': 'string',
        'email': 'string',
        'phone': 'string',
        'address': 'string',
        'segment': 'string',
        'country': 'string',
        'city': 'string',
        'state': 'string',
        'postal_code': 'long',
        'region': 'string'
    }
    
}
def format_data(df, df_mapping):
    """
    Input : df and data type mapping
    Output : df with correct data types
    Purpose: To format the data type as per mapping dict
    """
    for column in df_mapping.keys():
        if  type(df_mapping[column]) == list:
            df = df.withColumn(column, F.to_date(F.col(column), df_mapping[column][1])) 
        else:
            df = df.withColumn(column, F.col(column).cast(df_mapping[column]))
    return df

def drop_duplicates(df, pk_column, sort_key):
    """
    Input : input df , pk_column, sort_key to order the data
    Output : Dedupicated df
    Purpose: To remove duplicate using pk_column and sort_key , pick up the latest record by sort key
    """
    window_spec = Window.partitionBy(pk_column).orderBy(F.col(sort_key).desc())
    df = df.withColumn("rn", F.row_number().over(window_spec))
    df = df.filter(F.col("rn") == 1).drop("rn")
    return df

def create_raw_layer(product_df , order_df , customer_df):
    product_df = format_column_names(product_df)
    order_df = format_column_names(order_df)
    customer_df = format_column_names(customer_df)
    return product_df, order_df , customer_df
    


def create_enriched_layer(products_df_raw, orders_df_raw, customer_df_raw):
    
    products_df_raw = format_data(products_df_raw, column_mapping['product'])
    products_df_raw = drop_duplicates(products_df_raw, 'product_id', 'product_name')
    
    orders_df_raw = format_data(orders_df_raw, column_mapping['orders'])
        
    customer_df_raw = format_data(customer_df_raw, column_mapping['customer'])
    customer_df_raw = drop_duplicates(customer_df_raw, 'customer_id', 'customer_name')

    return products_df_raw, orders_df_raw , customer_df_raw


    

def create_fact_table(products_df_enriched, orders_df_enriched, customer_df_enriched):
    """
    Input : enriched product, order and customer data
    Output : fact order df
    Purpose: Join this 3 data set and round of profit by.2 decimal and select required columns
    """

    fact_df = orders_df_enriched\
        .join(customer_df_enriched, 'customer_id', 'left')\
        .join(products_df_enriched, 'product_id', 'left')\
        .withColumn('profit', F.round(F.col("profit"), 2))\
        .select(
            *[c for c in orders_df_enriched.columns],
            F.coalesce(customer_df_enriched['customer_name'], F.lit('UNKNOWN')).alias('customer_name'),
            F.coalesce(customer_df_enriched['country'], F.lit('UNKNOWN')).alias('country'), 
            F.coalesce(products_df_enriched['product_name'], F.lit('UNKNOWN')).alias('product_name'),
            F.coalesce(products_df_enriched['category'], F.lit('UNKNOWN')).alias('product_category'),
            F.coalesce(products_df_enriched['sub_category'], F.lit('UNKNOWN')).alias('product_sub_category') 
        ) 
    return fact_df 
    

def create_agg_table(fact_orders_df):
    """
    Input : fact order df
    Output : aggregated data
    Purpose: Aggregate data with sum of profiy by the required columns
    """
    agg_df = fact_orders_df\
        .withColumn('order_year', F.year(F.col('order_date')))\
        .groupBy('order_year', 'product_category', 'product_sub_category', 'customer_name')\
        .agg(F.round(F.sum(F.col("profit")), 2).alias('profit'))
    return agg_df

    

