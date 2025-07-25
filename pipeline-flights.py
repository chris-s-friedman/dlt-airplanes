import dlt
from dlt.sources.filesystem import filesystem, read_csv, read_csv_duckdb
import requests
from datetime import datetime
import zipfile
import io
import os
from glob import glob
import logging  # Added logging module

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

BASE_URL = 'https://transtats.bts.gov/PREZIP/On_Time_Marketing_Carrier_On_Time_Performance_Beginning_January_2018_'
UNZIP_DIR = './data/unzipped_files'
SPLIT_DIR = UNZIP_DIR + "_split"

# Start and end dates
start_year = 2018
start_month = 8

# Current year and month
current_year = datetime.now().year
current_month = datetime.now().month

# Calculate the year and month three months prior
if current_month <= 3:
    end_year = current_year - 1
    end_month = current_month + 9  # Wrap around to the previous year
else:
    end_year = current_year
    end_month = current_month - 3

# Generate the list of month-year combinations
month_year_list = []
for year in range(start_year, end_year + 1):
    if year == start_year:
        start_month_range = start_month
    else:
        start_month_range = 1
    for month in range(start_month_range, 13):
        if year == end_year and month > end_month:
            break
        month_year_list.append(f"{year}_{month}")

def generate_url(month_year, base_url=BASE_URL):
    """Generate the URL for a given month-year combination."""
    logging.info(f"Generating URL for {month_year}")
    return f"{BASE_URL}{month_year}.zip"

def get_zip_and_unzip(url, destination=UNZIP_DIR):
    """Download a zip file from the URL and unzip it to the destination."""
    try:
        logging.info(f"Downloading file from {url}...")
        response = requests.get(url)
        if response.status_code == 200:
            # Open the ZIP file in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # Extract all files to a directory
                zip_ref.extractall(destination)
            logging.info(f"File downloaded and extracted successfully to {destination}")
        else:
            logging.error(f"Failed to download file. HTTP Status Code: {response.status_code}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def split_csv(file_dir = UNZIP_DIR, output_dir = SPLIT_DIR , chunk_size=50000):
    """
        Split a large CSV file into smaller chunks. 
        Adapted from https://www.mungingdata.com/python/split-csv-write-chunk-pandas/
    """
    
    def write_chunk(part, lines):
        """Write a chunk of lines to a new CSV file."""
        logging.info(f"Writing chunk {part} with {len(lines)} lines to file.")
        with open(output_dir + "/" + str(part) +'.csv', 'w') as f_out:
            f_out.write(header)
            f_out.writelines(lines)
    
    flight_csv = glob(os.path.join(file_dir, "*.csv"))[0]  # Assuming there's only one CSV file in the directory
    
    with open(flight_csv, 'r') as f_in:
        count = 0
        header = f_in.readline()  # Read the header line
        lines = []
        for line in f_in:
            lines.append(line)
            count += 1
            if count % chunk_size == 0:
                write_chunk(count // chunk_size, lines)
                lines = []
        if lines:  # Write any remaining lines
            write_chunk(count // chunk_size + 1, lines)

def delete_files_in_directory(directory):
    """Delete all files in the specified directory."""
    for file_path in glob(os.path.join(directory, '*')):
        try:
            os.remove(file_path)
            logging.info(f"Deleted file: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting file {file_path}: {e}")

def run_pipeline(month_year, unzip_dir = UNZIP_DIR, file_dir=SPLIT_DIR, fetch=True):
    """Run the pipeline for a specific month-year."""
    if fetch:
        logging.info(f"Fetching data for {month_year}...")
        url = generate_url(month_year)
        get_zip_and_unzip(url)
        split_csv(file_dir=unzip_dir, output_dir=file_dir)  # Split the CSV files into smaller chunks
        delete_files_in_directory(unzip_dir)  # Clean up unzipped files after splitting
        logging.info(f"Data for {month_year} fetched and split.")
    else:
        logging.info(f"Skipping fetch for {month_year}...")

    pipeline = dlt.pipeline(
        pipeline_name=month_year + "_flights",
        destination='postgres',
        dataset_name="src_flights",
    )
    
    for file in os.listdir(file_dir):
        logging.info(f"Processing file: {file}")
        # Create a filesystem source for the file
        fs = (filesystem(bucket_url=file_dir, file_glob=file) | read_csv()).with_name("flights")
        info = pipeline.run(fs, write_disposition="append")
        
    delete_files_in_directory(file_dir)  # Clean up files after processing
    logging.info(f"Pipeline run for {month_year} completed with info: {info}")

for month_year in month_year_list:
    # check if the month and year are already in the database
    run_pipeline(month_year, fetch=True)  # Fetch data for each month-year
    logging.info(f"Completed processing for {month_year}.\n")

