from pyspark.sql.types import *
import datetime

##   test_read_table 
test_table_schema = StructType([ \
StructField("id",     IntegerType(), True), \
StructField("name",   StringType(),   True)
])
test_data = [ 
    (1, 'Ram'),
    (2, 'Shyam') 
]

## test_format_column_names
raw_products_schema = StructType([
    StructField('Product ID', StringType(), True),
    StructField('Category', StringType(), True), 
    StructField('Sub-Category', StringType(), True), 
    StructField('Product Name', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Price per product', DoubleType(), True)
])
raw_products_data = [
('FUR-CH-10002961','Furniture','Chairs','Leather Task Chair, Black','New York',81.882),
('TEC-AC-10004659','Technology','Accessories','Hardware USB 2.0\xa0Flash Drive; 16GB','Oklahoma',72.99)
]
format_column_expected_column_name = ['product_id', 'category', 'sub_category', 'product_name', 'state', 'price_per_product']

## test_format_data
enriched_orders_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('discount', DoubleType(), True), 
    StructField('order_date', StringType(), True), 
    StructField('order_id', StringType(), True), 
    StructField('price', DoubleType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('profit', DoubleType(), True), 
    StructField('quantity', LongType(), True), 
    StructField('row_id', LongType(), True), 
    StructField('ship_date', StringType(), True), 
    StructField('ship_mode', StringType(), True)])
enriched_data = [
    ('JK-15370',0.3,'21/8/2016','CA-2016-122581',573.17,'FUR-CH-10002961',63.69,7,1,'25/8/2016','Standard Class'),
    ('BD-11320',0.0,'23/9/2017','CA-2017-117485',291.96,'TEC-AC-10004659',102.19,4,2,
        '29/9/2017','Standard Class')
]
format_data_expected_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('discount', DoubleType(), True), 
    StructField('order_date', DateType(), True), 
    StructField('order_id', StringType(), True), 
    StructField('price', DoubleType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('profit', DoubleType(), True), 
    StructField('quantity', LongType(), True), 
    StructField('row_id', LongType(), True), 
    StructField('ship_date', DateType(), True), 
    StructField('ship_mode', StringType(), True)])

format_data_expected_data = [
('JK-15370',0.3,datetime.date(2016, 8, 21),'CA-2016-122581',573.17,'FUR-CH-10002961',63.69,7,1,datetime.date(2016, 8, 25),'Standard Class'),
('BD-11320',0.0,datetime.date(2017, 9, 23),'CA-2017-117485',291.96,'TEC-AC-10004659',102.19,4,2,datetime.date(2017, 9, 29),'Standard Class')
]

## test_drop_duplicates

drop_test_table_schema = StructType([ 
    StructField("id",     IntegerType(), True), 
    StructField("name",   StringType(),   True)
])
drop_test_data = [ 
    (1, 'Ram'),
    (2, 'Shyam'),
    (2, 'Abi')  
]

## test_create_fact_table

fact_products_schema = StructType([
    StructField('product_id', StringType(), True),
    StructField('category', StringType(), True), 
    StructField('sub_category', StringType(), True), 
    StructField('product_name', StringType(), True),
    StructField('state', StringType(), True),
    StructField('price_per_product', DoubleType(), True)
])
fact_products_data = [
    ('FUR-CH-10002961','Furniture','Chairs','Leather Task Chair, Black','New York',81.882),
    ('TEC-AC-10004659','Technology','Accessories','Hardware USB 2.0\xa0Flash Drive; 16GB','Oklahoma',72.99)
]

fact_orders_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('discount', DoubleType(), True), 
    StructField('order_date', DateType(), True), 
    StructField('order_id', StringType(), True), 
    StructField('price', DoubleType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('profit', DoubleType(), True), 
    StructField('quantity', LongType(), True), 
    StructField('row_id', LongType(), True), 
    StructField('ship_date', DateType(), True), 
    StructField('ship_mode', StringType(), True)])

fact_order_data = [
    ('JK-15370',0.3,datetime.date(2016, 8, 21),'CA-2016-122581',573.17,'FUR-CH-10002961',63.622229,7,1,datetime.date(2016, 8, 25),'Standard Class'),
    ('BD-113201',0.0,datetime.date(2017, 9, 23),'CA-2017-117485',291.96,'TEC-AC-100046591',102.193,4,2,datetime.date(2017, 9, 29),'Standard Class')
]
fact_custmer_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('customer_name', StringType(), True), 
    StructField('email', StringType(), True), 
    StructField('phone', StringType(), True), 
    StructField('address', StringType(), True), 
    StructField('segment', StringType(), True), 
    StructField('country', StringType(), True), 
    StructField('city', StringType(), True), 
    StructField('state', StringType(), True),
    StructField('postal_code', LongType(), True), 
    StructField('region', StringType(), True)
    ])
fact_customer_Data = [
    ('JK-15370','Alex Avila','josephrice131@gmail.com','680-261-2092',
    '91773 Miller Shoal\nDiaztown, FL 38841','Consumer','United States','Round Rock','Texas',
    78664,'Central'),
    ('BD-11320','Allen Armold','garymoore386@gmail.com','221.945.4191x8872',
    '6450 John Lodge\nTerriton, KY 95945','Consumer','United States','Atlanta',
    'Georgia',30318,'South')
]
fact_expected_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('discount', DoubleType(), True), 
    StructField('order_date', DateType(), True), 
    StructField('order_id', StringType(), True), 
    StructField('price', DoubleType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('profit', DoubleType(), True), 
    StructField('quantity', LongType(), True), 
    StructField('row_id', LongType(), True), 
    StructField('ship_date', DateType(), True), 
    StructField('ship_mode', StringType(), True), 
    StructField('customer_name', StringType(), True), 
    StructField('country', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('product_category', StringType(), True), 
    StructField('product_sub_category', StringType(), True)
])
fact_expected_data = [
    ('JK-15370',0.3,datetime.date(2016, 8, 21),'CA-2016-122581',573.17,'FUR-CH-10002961',63.62,7,1,datetime.date(2016, 8, 25),'Standard Class','Alex Avila','United States',
    'Leather Task Chair, Black','Furniture','Chairs'),
    ('BD-113201',0.0,datetime.date(2017, 9, 23),'CA-2017-117485',291.96,'TEC-AC-100046591',
        102.19,4,2,datetime.date(2017, 9, 29),'Standard Class','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN')
]


##. test_create_agg_table

agg_fact_schema = StructType([
    StructField('customer_id', StringType(), True), 
    StructField('discount', DoubleType(), True), 
    StructField('order_date', DateType(), True), 
    StructField('order_id', StringType(), True), 
    StructField('price', DoubleType(), True), 
    StructField('product_id', StringType(), True), 
    StructField('profit', DoubleType(), True), 
    StructField('quantity', LongType(), True), 
    StructField('row_id', LongType(), True), 
    StructField('ship_date', DateType(), True), 
    StructField('ship_mode', StringType(), True), 
    StructField('customer_name', StringType(), True), 
    StructField('country', StringType(), True), 
    StructField('product_name', StringType(), True), 
    StructField('product_category', StringType(), True), 
    StructField('product_sub_category', StringType(), True)
])
agg_fact_data = [
    ('JK-15370',0.3,datetime.date(2016, 8, 21),'CA-2016-122581',573.17,'FUR-CH-10002961',63.62,7,1,datetime.date(2016, 8, 25),'Standard Class','Alex Avila','United States',
    'Leather Task Chair, Black','Furniture','Chairs'),
    ('BD-113201',0.0,datetime.date(2017, 9, 23),'CA-2017-117485',291.96,'TEC-AC-100046591',
    102.19,4,2,datetime.date(2017, 9, 29),'Standard Class','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN')
]
agg_data_expected = [
    (2016, 'Furniture', 'Chairs', 'Alex Avila', 63.62),
    (2017, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 102.19)
]
agg_schema_expected = StructType([
    StructField('order_year', IntegerType(), True),
    StructField('product_category', StringType(), True), 
    StructField('product_sub_category', StringType(), True), 
    StructField('customer_name', StringType(), True), 
    StructField('profit', DoubleType(), True)
])

