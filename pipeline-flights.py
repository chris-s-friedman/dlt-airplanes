import dlt
from dlt.sources.filesystem import filesystem, read_csv

# download link for april 2025:
# https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGK&QO_fu146_anzr=b0-gvzr

pipeline = dlt.pipeline(
        pipeline_name="load_airframes",
        destination='postgres',
        dataset_name="src_airframes",
    )


BUCKET = "./data"

files = [
    "ACFTREF",
    "DEALER", 
    #"DEREG", 
    "DOCINDEX",
    "ENGINE", 
    "MASTER", 
    "RESERVED"
]


for file in files:
        fs = (filesystem(bucket_url=BUCKET, file_glob = f"*{file}.txt") | read_csv()).with_name(file.lower())
        info = pipeline.run(fs, write_disposition = "replace")
        print(info)
