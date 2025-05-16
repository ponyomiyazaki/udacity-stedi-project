import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import re

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

# Script generated for node customer_landing
customer_landing_node1747402118591 = glueContext.create_dynamic_frame.from_catalog(database="udacity-project", table_name="customer_landing", transformation_ctx="customer_landing_node1747402118591")

# Script generated for node privacy_filter
privacy_filter_node1747402144938 = Filter.apply(frame=customer_landing_node1747402118591, f=lambda row: (not(row["sharewithresearchasofdate"] == 0)), transformation_ctx="privacy_filter_node1747402144938")

# Script generated for node customer_trusted
EvaluateDataQuality().process_rows(frame=privacy_filter_node1747402144938, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747401899177", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_trusted_node1747402170200 = glueContext.getSink(path="s3://udacity-project-work-yw/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1747402170200")
customer_trusted_node1747402170200.setCatalogInfo(catalogDatabase="udacity-project",catalogTableName="customer_trusted")
customer_trusted_node1747402170200.setFormat("json")
customer_trusted_node1747402170200.writeFrame(privacy_filter_node1747402144938)
job.commit()
