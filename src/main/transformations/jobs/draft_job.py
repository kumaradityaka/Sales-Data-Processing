import datetime
import shutil
import os
import sys

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, IntegerType, DateType, FloatType, StringType, StructField

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.transformations.jobs.sale_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

# Initialize S3 client
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()


def extract_data():
    """
    Extract data from S3, download files, and validate them.
    """
    try:
        # List files in S3 bucket
        s3_reader = S3Reader()
        folder_path = config.s3_source_directory
        s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
        logger.info("Absolute path on S3 bucket for CSV files: %s", s3_absolute_file_path)

        if not s3_absolute_file_path:
            logger.info(f"No files available at {folder_path}")
            raise Exception("No data available to process")

        # Download files from S3
        bucket_name = config.bucket_name
        prefix = f"s3://{bucket_name}/"
        file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
        logger.info(f"File paths available on S3 under {bucket_name} bucket")

        downloader = S3FileDownloader(s3_client, bucket_name, config.local_directory)
        downloader.download_files(file_paths)

        # Validate downloaded files
        all_files = os.listdir(config.local_directory)
        logger.info(f"List of files downloaded: {all_files}")

        if not all_files:
            logger.error("No CSV data available")
            raise Exception("No data to process")

        csv_files = []
        error_files = []
        for file in all_files:
            if file.endswith(".csv"):
                csv_files.append(os.path.abspath(os.path.join(config.local_directory, file)))
            else:
                error_files.append(os.path.abspath(os.path.join(config.local_directory, file)))

        logger.info("List of CSV files to be processed: %s", csv_files)
        logger.info("List of error files: %s", error_files)

        return csv_files, error_files

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise e


def transform_data(csv_files, error_files):
    """
    Transform data: validate schema, handle errors, and prepare data for loading.
    """
    try:
        spark = spark_session()
        logger.info("Spark session created")

        # Validate schema of CSV files
        correct_files = []
        for file in csv_files:
            data_schema = spark.read.format("csv").option("header", "true").load(file).columns
            missing_columns = set(config.mandatory_columns) - set(data_schema)
            if missing_columns:
                error_files.append(file)
            else:
                correct_files.append(file)

        logger.info(f"Correct files: {correct_files}")
        logger.info(f"Error files: {error_files}")

        # Move error files to error directory
        error_folder_local_path = config.error_folder_path_local
        if error_files:
            for file_path in error_files:
                if os.path.exists(file_path):
                    file_name = os.path.basename(file_path)
                    destination_path = os.path.join(error_folder_local_path, file_name)
                    shutil.move(file_path, destination_path)
                    logger.info(f"Moved '{file_name}' to '{destination_path}'")
                    source_prefix = config.s3_source_directory
                    destination_prefix = config.s3_error_directory
                    message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
                    logger.info(f"{message}")
                else:
                    logger.error(f"'{file_path}' does not exist.")

        # Prepare final DataFrame for processing
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

        final_df_to_process = spark.createDataFrame([], schema)
        for file in correct_files:
            data_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file)
            extra_columns = list(set(data_df.columns) - set(config.mandatory_columns))
            if extra_columns:
                data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
                                 .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                                         "price", "quantity", "total_cost", "additional_column")
            else:
                data_df = data_df.withColumn("additional_column", lit(None)) \
                                 .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id",
                                         "price", "quantity", "total_cost", "additional_column")
            final_df_to_process = final_df_to_process.union(data_df)

        logger.info("Final DataFrame for processing:")
        final_df_to_process.show()

        return final_df_to_process, correct_files

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise e


def load_data(final_df_to_process, correct_files):
    """
    Load data into data marts, update database, and clean up.
    """
    try:
        spark = spark_session()

        # Join dimension tables
        database_client = DatabaseReader(config.url, config.properties)
        customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
        store_table_df = database_client.create_dataframe(spark, config.store_table)
        sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

        s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process, customer_table_df,
                                                                store_table_df, sales_team_table_df)

        # Write to Customer Data Mart
        final_customer_data_mart_df = s3_customer_store_sales_df_join.select(
            "ct.customer_id", "ct.first_name", "ct.last_name", "ct.address",
            "ct.pincode", "phone_number", "sales_date", "total_cost")
        parquet_writer = ParquetWriter("overwrite", "parquet")
        parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

        # Upload to S3
        s3_uploader = UploadToS3(s3_client)
        s3_directory = config.s3_customer_datamart_directory
        message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
        logger.info(f"{message}")

        # Write to Sales Data Mart
        final_sale_team_data_mart_df = s3_customer_store_sales_df_join.select(
            "store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name",
            "store_manager_name", "manager_id", "is_manager", "sales_person_address",
            "sales_person_pincode", "sales_date", "total_cost",
            expr("SUBSTRING(sales_date, 1, 7) as sales_month"))
        parquet_writer.dataframe_writer(final_sale_team_data_mart_df, config.sales_team_data_mart_local_file)

        # Move S3 files to processed directory
        source_prefix = config.s3_source_directory
        destination_prefix = config.s3_processed_directory
        message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
        logger.info(f"{message}")

        # Clean up local files
        delete_local_file(config.local_directory)
        delete_local_file(config.customer_data_mart_local_file)
        delete_local_file(config.sales_team_data_mart_local_file)

        # Update staging table
        update_statements = []
        current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for file in correct_files:
            filename = os.path.basename(file)
            statements = f"""UPDATE {config.database_name}.{config.product_staging_table}
                             SET status = 'I', updated_date = '{current_date}'
                             WHERE file_name = '{filename}'"""
            update_statements.append(statements)

        connection = get_mysql_connection()
        cursor = connection.cursor()
        for statement in update_statements:
            cursor.execute(statement)
            connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise e

def sales_etl_job():
    """
    Main function to execute the ETL process.
    """
    try:
        # Extract
        csv_files, error_files = extract_data()

        # Transform
        final_df_to_process, correct_files = transform_data(csv_files, error_files)

        # Load
        load_data(final_df_to_process, correct_files)

    except Exception as e:
        logger.error(f"ETL job failed: {e}")
        sys.exit()

# Execute the ETL job
sales_etl_job()