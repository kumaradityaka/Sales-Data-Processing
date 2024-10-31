import datetime
import os
import shutil

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, IntegerType, DateType, FloatType, StringType, StructField

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import  dimensions_table_join
from src.main.transformations.jobs.sale_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

aws_access_key = config.aws_access_key  #get s3 access key
aws_secret_key = config.aws_secret_key  #secret key
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response)

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
conn = get_mysql_connection()
cursor = conn.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    statement = f"select distinct file_name from " \
                f"{config.database_name}.{config.product_staging_table} " \
                f"where file_name in ({str(total_csv_files)[1:-1]}) and status='A' "
    logger.info(f"SQL Query: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("No Match")
else:
    logger.info("Last run was successful!")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path=folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s ", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No Files available at {folder_path}")
        raise Exception("No Data available to process")
except Exception as e:
    logger.error("Excited with error: %s", e)
    raise e

bucket_name = config.bucket_name
prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info("File path available on s3 under %s bucket and folder name is %s", bucket_name)
logger.info(f"File path available on s3 under {bucket_name} bucket and folder")
try:
    downloader = S3FileDownloader(s3_client, bucket_name, config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File Download failed: %s", e)
    sys.exit()

all_files = os.listdir(config.local_directory)
logger.info(f"List of files downloaded {all_files}")

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory, files)))
else:
    logger .error("No csv data available")
    raise Exception("There is no data to process")


logger.info("**************Listing the File**************")
logger.info("List of csv files needs to be processed %s", csv_files)
logger.info("***************Creating spark session**************")
spark = spark_session()
logger.info("*********Spark session created**********")

logger.info("******** Checking schema ******")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv") \
                    .option("header","true") \
                    .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory column schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")
    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(f"List of correct files {correct_files}")
logger.info(f"List of error files {error_files}")
logger.info("Moving Error data to error dir if any")

error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}' ")
            source_prefix  = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix,destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist. ")

else:
    logger.info("***No File Available at our data source***")

logger.info(f"****Updating the product_staging_Table")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""
                        INSERT INTO {db_name}.{config.product_staging_table}
                        (file_name, file_location, created_date, status)
                        VALUES('{filename}', '{filename}', '{formatted_date}', 'A')
                        """
        insert_statements.append(statements)
    logger.info(f"Insert statements created for staging table---{insert_statements}")
    logger.info("*******Connecting with MySql Server")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("*****Connected to Database")
    for stmt in insert_statements:
        cursor.execute(stmt)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("****There are no files to process****")
    raise Exception("****No Data available with correct files****")
logger.info("******* Fixing extra column coming from source********")
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])
database_client = DatabaseReader(config.url, config.properties)
final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")
# final_df_to_process = spark.createDataFrame(empty_rdd, schema=schema)
for data in correct_files:
    data_df = spark.read.format("csv") \
            .option("header", "True") \
            .option("inferSchema", "true") \
            .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source are {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
                    .select("customer_id", "store_id", "product_name", "sales_date","sales_person_id",
                            "price", "quantity", "total_cost", "additional_column")
        logger.info(f"processed {data} and added 'additional_column' ")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                    "price", "quantity", "total_cost", "additional_column")
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("*********Final Dataframe from source which will be going to processing******")
final_df_to_process.show()

#connecting with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

logger.info("Creating dfs for each table")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)
product_table_df = database_client.create_dataframe(spark, config.product_table)

product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
store_table_df = database_client.create_dataframe(spark, config.store_table)
logger.info("Created Dataframes for processing..")

s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)
logger.info("******Writing data into Customer Data mart*******")
final_customer_Data_mart_df =s3_customer_store_sales_df_join.select(
                            "ct.customer_id", "ct.first_name","ct.last_name", "ct.address",
                            "ct.pincode", "phone_number", "sales_date", "total_cost")
logger.info("******Final Data fo Customer*****")
final_customer_Data_mart_df.show()

parquet_writer = ParquetWriter("overwrite",  "parquet")
parquet_writer.dataframe_writer(final_customer_Data_mart_df,config.customer_data_mart_local_file)
logger.info(f"Customer data written at location {config.customer_data_mart_local_file}")

logger.info("From local to data mart")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name,
                                   config.customer_data_mart_local_file)
logger.info(f"{message}")

logger.info("******Sales datamart creation started******")
final_sale_team_data_mart_df = s3_customer_store_sales_df_join \
                                .select("store_id", "sales_person_id",
                                        "sales_person_first_name", "sales_person_last_name",
                                        "store_manager_name", "manager_id", "is_manager",
                                        "sales_person_address", "sales_person_pincode",
                                        "sales_date", "total_cost",
                                        expr("SUBSTRING(sales_date,1,7) as sales_month"))
logger.info("****Sales final data created****")
final_sale_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sale_team_data_mart_df, config.sales_team_data_mart_local_file)

final_sale_team_data_mart_df.write.format("parquet")\
    .option("header", "true")\
    .mode("overwrite")\
    .partitionBy("sales_month", "store_id")\
    .option("path", config.sales_team_data_mart_partitioned_local_file)\
    .save()
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

logger.info("Calculating purchase amount for every month")
customer_mart_calculation_table_write(final_customer_Data_mart_df)

logger.info("Calculating incentive...")
sales_mart_calculation_table_write(final_sale_team_data_mart_df)

#mOVING s3 FILE into processed folder and delte local file
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("Deleting files from local....")
delete_local_file(config.local_directory)
delete_local_file(config.customer_data_mart_local_file)
delete_local_file(config.sales_team_data_mart_local_file)

#update the status of staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""UPDATE {db_name}.{config.product_staging_table}
                        set status = 'I', updated_date = '{formatted_date}'
                        where file_name = '{filename}'"""
        update_statements.append(statements)
    logger.info(f"Updated statement created for staging table -- {update_statements}")
    logger.info("Connecting to MySql.....")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("Mysql server connected successfully connected...")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("******There is some error in th process")
    sys.exit()

input("Enter to terminate")
