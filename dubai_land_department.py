import os
import subprocess
from datetime import date
from dotenv import load_dotenv
import logging

from lib.extract.rent_contracts_downloader import RentContractsDownloader
from lib.logging_helpers import get_logger, configure_root_logger
from lib.transform.rent_contracts_transformer import RentContractsTransformer
from lib.classes.property_usage import PropertyUsage
from lib.workspace.github_client import GitHubRelease

load_dotenv()

configure_root_logger(logfile="etl.log", loglevel="DEBUG")
logger = get_logger("ETL")


def download_rent_contracts(url, filename):
    logger.info("Downloading rent contracts")
    if os.path.isfile(filename):
        logger.info(f"{filename} already exists. Skipping download.")
        return

    logger.info(f"{filename} not found. Running RentContractsDownloader.")
    try:
        downloader = RentContractsDownloader(url)
        downloader.run(filename)
    except Exception as e:
        logger.error(f"Error downloading rent contracts: {e}")
        raise

def transform_rent_contracts(input_file, output_file):
    if not os.path.isfile(input_file):
        logger.error(f"{input_file} not found. Cannot transform.")
        return
    if not os.path.isfile(output_file):
        logger.info(f"Transforming {input_file} to {output_file}.")
        try:
            transformer = RentContractsTransformer(input_file, output_file)
            transformer.transform()
        except Exception as e:
            logger.error(f"Error transforming rent contracts: {e}")
            raise
    else:
        logger.info(f"{output_file} exists")

def get_property_usage(input_file, output_file):
    if not os.path.isfile(input_file):
        logger.error(f"{input_file} not found. Cannot get property usage.")
        return
    try:
        property_usage = PropertyUsage(output_file)
        property_usage.transform(input_file)
        logger.info(f"Property usage saved to {output_file}")
    except Exception as e:
        logger.error(f"Error getting property usage: {e}")
        raise
    else:
        logger.info(f"{output_file} exists")


def publish_to_github_release(files):
    """
    Data files uploads to GitHub Release
    """
    try:
        publisher = GitHubRelease('ggurjar333/real-estate-analysis-dubai')
        publisher.publish(files=files)

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")    

def main():
    url = os.getenv("DLD_URL")
    if not url:
        logger.error("DLD_URL environment variable not set.")
        return

    csv_filename = f'output/rent_contracts_{date.today()}.csv'
    parquet_filename = f'dld_rent_contracts_{date.today()}.parquet'
    property_usage_report_file = f'property_usage_report_{date.today()}.csv'

    release_checker = GitHubRelease('ggurjar333/real-estate-analysis-dubai')
    release_name = f'release-{date.today()}'
    
    if not release_checker.release_exists(release_name):
        download_rent_contracts(url, csv_filename)
        transform_rent_contracts(csv_filename, parquet_filename)
        get_property_usage(parquet_filename, property_usage_report_file)
        publish_to_github_release([parquet_filename, property_usage_report_file])
    else:
        print(f"Release '{release_name}' already exists. No action needed.")
            

if __name__ == "__main__":
    main()
