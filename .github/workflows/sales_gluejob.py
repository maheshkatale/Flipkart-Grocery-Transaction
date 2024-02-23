import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
salesDF = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://flipkart-grocery-sales"], "recurse": True},
    transformation_ctx="salesDF",
)

# Script generated for node Change Schema
salesDF = ApplyMapping.apply(
    frame=salesDF,
    mappings=[
        ("date_", "string", "Date", "date"),
        ("city_name", "string", "City", "string"),
        ("order_id", "string", "order_id", "int"),
        ("cart_id", "string", "cart_id", "int"),
        ("dim_customer_key", "string", "dim_customer_key", "int"),
        ("procured_quantity", "string", "procured_quantity", "smallint"),
        ("unit_selling_price", "string", "unit_selling_price", "decimal"),
        ("total_discount_amount", "string", "total_discount_amount", "decimal"),
        ("product_id", "string", "product_id", "int"),
        ("total_weighted_landing_price","string","total_weighted_landing_price","decimal",),
    ],
    transformation_ctx="salesDF",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT
    *,
    SUM(procured_quantity) OVER (PARTITION BY product_id) AS Total_Quantity_Sold,
    SUM(procured_quantity * unit_selling_price) OVER (PARTITION BY product_id) AS Total_Sales,
    (procured_quantity * unit_selling_price - total_discount_amount) AS Total_Revenue,
    ROUND((total_discount_amount / (unit_selling_price * procured_quantity)) * 100) AS Discount_Percentage,
    ROUND(((((procured_quantity * unit_selling_price)-total_discount_amount) - total_weighted_landing_price) / (procured_quantity * unit_selling_price)) * 100) AS Profit_Margin
FROM
    myDataSource;
"""
salesDF = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": salesDF},
    transformation_ctx="salesDF",
)

# Script generated for node Amazon S3
sales_data_sink = glueContext.getSink(
    path="s3://flipkart-outputs/sales_output/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",  # Using Glue Job Bookmark for update behavior
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="sales_data_sink",
)
sales_data_sink.setCatalogInfo(
    catalogDatabase="flipkart_database", catalogTableName="sales"
)
sales_data_sink.setFormat("glueparquet", compression="uncompressed")
sales_data_sink.writeFrame(salesDF)

# Commit the job
job.commit()
