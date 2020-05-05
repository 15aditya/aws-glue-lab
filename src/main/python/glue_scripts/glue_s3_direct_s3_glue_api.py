"""
This script reads data from s3 and load data into another folder at s3 using glue api's
"""

# import necessary dependencies
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# extract source s3 file as glue dynamic frame using data catalogue.
dynf_s3_src = glueContext.create_dynamic_frame.from_catalog(database='ctg_db_s3',
                                                            table_name='ctg_tbl_nm_corresponding_to_s3_file',
                                                            transformation_ctx="s3_src_extract")

# load s3 file using glue api.
glueContext.write_dynamic_frame_from_options(frame=dynf_s3_src,
                                             connection_type="s3",
                                             connection_options={"path": 's3://bucket_name/path'},
                                             format="parquet",
                                             format_options={},
                                             transformation_ctx="")

job.commit()

