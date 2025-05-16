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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1747438780799 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1747438780799")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1747438853999 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1747438853999")

# Script generated for node Join
Join_node1747438866077 = Join.apply(frame1=accelerometer_trusted_node1747438780799, frame2=step_trainer_trusted_node1747438853999, keys1=["timestamp"], keys2=["sensorreadingtime"], transformation_ctx="Join_node1747438866077")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=Join_node1747438866077, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747438775513", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1747439029952 = glueContext.getSink(path="s3://udacity-project-work-yw/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1747439029952")
machine_learning_curated_node1747439029952.setCatalogInfo(catalogDatabase="udacity-project",catalogTableName="machine_learning_curated")
machine_learning_curated_node1747439029952.setFormat("json")
machine_learning_curated_node1747439029952.writeFrame(Join_node1747438866077)
job.commit()
