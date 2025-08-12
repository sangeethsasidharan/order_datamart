
**Assumptions**
- Assuming product_id is the unique business key in products file, if any duplicate will pick up first product sorterd by dec 
- Same for customer_id in customer file , If found duplicates will pick up the first customer sorted by customer name
- Seen some customer names with special charters eg "Fra9876nk Gasti  ;.,.,neau", Since no insturction given on how to handle this special chareters , For now leaving as is, If we required can remove such special character
- During join to prepare fact_orders for non matching customer and product join , Lookup fields have kept 'UNKNOWN' since not specified what to do with this, IF required i can handle accordigily and filter out this records, 
- For fact_order prepration applied left join and not removed any non joining records
- To read excel source right approch would be to use this "https://medium.com/@amitjoshi7/how-to-read-excel-files-using-pyspark-in-databricks-637bb21b90be" but in databricks free edition we can not install any maven depdency i have used this "https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/pyspark.pandas.read_excel.html" 



Below are the layers and transformations

Actual data saved in '**Prod**' schema

**RAW layer**
Column names are formatted , below tables created with as is data 
- raw_products 
- raw_orders
- raw_customers

**Enriched layer**
Data types are formatted , Duplicate records are removed, Below are the tables
- enriched_products
- enriched_orders
- enriched_customers

**Fact layer**
3 data set combined to create fact table with required lookup columns from the product and customer table
Profit is rounded to 2 decimal

**Agg layer**
Fact order profit data aggregated by required fields  
