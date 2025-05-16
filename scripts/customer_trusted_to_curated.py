import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer_trusted
customer_trusted_node1747432274113 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1747432274113")

# Script generated for node accelerometer_landing
accelerometer_landing_node1747432294628 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1747432294628")

# Script generated for node Join
Join_node1747435769712 = Join.apply(frame1=customer_trusted_node1747432274113, frame2=accelerometer_landing_node1747432294628, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1747435769712")

# Script generated for node SQL Query
SqlQuery1177 = '''
select distinct * from myDataSource
where sharewithresearchasofdate is not null

'''
SQLQuery_node1747435795638 = sparkSqlQuery(glueContext, query = SqlQuery1177, mapping = {"myDataSource":Join_node1747435769712}, transformation_ctx = "SQLQuery_node1747435795638")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747435795638, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747434975614", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747435839251 = glueContext.getSink(path="s3://udacity-project-work-yw/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747435839251")
AmazonS3_node1747435839251.setCatalogInfo(catalogDatabase="udacity-project",catalogTableName="customer_curated")
AmazonS3_node1747435839251.setFormat("json")
AmazonS3_node1747435839251.writeFrame(SQLQuery_node1747435795638)
job.commit()
