import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node accelerometer_landing
accelerometer_landing_node1747429744512 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1747429744512")

# Script generated for node customer_trusted
customer_trusted_node1747429831295 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1747429831295")

# Script generated for node Join
Join_node1747430219554 = Join.apply(frame1=accelerometer_landing_node1747429744512, frame2=customer_trusted_node1747429831295, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1747430219554")

# Script generated for node Drop Fields
DropFields_node1747434290785 = DropFields.apply(frame=Join_node1747430219554, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1747434290785")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1747430219554, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747429735665", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747430348211 = glueContext.getSink(path="s3://udacity-project-work-yw/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747430348211")
AmazonS3_node1747430348211.setCatalogInfo(catalogDatabase="udacity-project",catalogTableName="accelerometer_trusted")
AmazonS3_node1747430348211.setFormat("json")
AmazonS3_node1747430348211.writeFrame(Join_node1747430219554)
job.commit()
