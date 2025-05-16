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

# Script generated for node customer_curated
customer_curated_node1747436822461 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="customer_curated", transformation_ctx="customer_curated_node1747436822461")

# Script generated for node step_trainer_landing
step_trainer_landing_node1747436848375 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1747436848375")

# Script generated for node Join
Join_node1747436896589 = Join.apply(frame1=step_trainer_landing_node1747436848375, frame2=customer_curated_node1747436822461, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1747436896589")

# Script generated for node Drop Fields
DropFields_node1747436921230 = DropFields.apply(frame=Join_node1747436896589, paths=["`.serialnumber`", "birthday", "sharewithpublicasofdate", "sharewithresearchasofdate", "registrationdate", "customername", "sharewithfriendsasofdate", "email", "lastupdatedate", "phone"], transformation_ctx="DropFields_node1747436921230")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1747436921230, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747436798823", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747437035903 = glueContext.getSink(path="s3://udacity-project-work-yw/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747437035903")
AmazonS3_node1747437035903.setCatalogInfo(catalogDatabase="udacity-project",catalogTableName="step_trainer_trusted")
AmazonS3_node1747437035903.setFormat("json")
AmazonS3_node1747437035903.writeFrame(DropFields_node1747436921230)
job.commit()
