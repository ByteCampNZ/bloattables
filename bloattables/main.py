import csv
import datetime
from io import StringIO
from pathlib import Path
import random
from typing import List, Optional, TypeVar

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pandera as pa
import numpy


__version__: str = '0.1.0'


T: TypeVar = TypeVar('T')


today: pd.Timestamp = pd.Timestamp.today()
earliest_permissible_dob: pd.Timestamp = pd.Timestamp('1920-01-01')


def load_data(filename: str) -> List[str]:
    """Takes the individual lines within a text file and returns the
    contents as a list.

    Args:
        filename: The path to the text file.

    Returns:
        A list of string values taken from the text file.

    """
    with open(Path("bloattables/assets", filename)) as f:
        return f.read().splitlines()


def sample_triangular(items: List[T]) -> T:
    """Uses a list of items and samples from a triangular sample where
    the mode is the first index.

    Args:
        items: The items from which to be sampled.

    Returns:
        The result from the sample.

    """
    return items[int(numpy.random.triangular(left=0, mode=0, right=len(items)))]


def generate_data() -> pd.DataFrame:
    """Generates a large quantity of test data with person details.

    Returns:
        A pandas DataFrame with all the test data.

    """
    # Prepares to store a CSV file in memory.
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(['person_id', 'fname', 'lname', 'sex', 'date_of_birth'])

    # Loads each list of names.
    male_fnames = load_data('fnames/male')
    female_fnames = load_data('fnames/female')
    lnames = load_data('lnames/lnames')

    # Adds new rows of data to the CSV
    for no_row in range(100_000):
        df_id = no_row + 1
        sex = random.choice(('male', 'female'))
        fname = sample_triangular(male_fnames if sex == 'male' else female_fnames)
        lname = sample_triangular(lnames)
        days_old = random.randint(a=1, b=32850)
        date_of_birth = today - datetime.timedelta(days=days_old)

        # Writes the
        csv_writer.writerow([df_id, fname, lname, sex, date_of_birth])

    output.seek(0)
    return pd.read_csv(output, parse_dates=['date_of_birth'])


def check_data(df: pd.DataFrame) -> pd.DataFrame:
    """Verifies that the test data is usable and contains no unexpected
    values, raising an exception if not.

    Args:
        df: The test data.

    Returns:
        The verified data frame.

    """
    schema = pa.DataFrameSchema({
        'person_id': pa.Column(int, pa.Check.greater_than(0)),
        'fname': pa.Column(str),
        'lname': pa.Column(str),
        'sex': pa.Column(str, checks=[pa.Check.str_matches('male|female')]),
        'date_of_birth': pa.Column(pa.dtypes.DateTime, pa.Check(
            lambda dt: pd.Timestamp('1920-01-01') <= dt <= today, element_wise=True
        ))
    })

    return schema(df)


def bucket_push(file_name: str | Path, bucket: str, object_name: Optional[str] = None) -> bool:
    """Upload a file to an S3 bucket.

    Args:
        file_name: The path to the file to be uploaded.
        bucket: The name of the bucket to be uploaded to.
        object_name: The name of the object to be stored in the bucket.

    Returns:
        A boolean for whether or not the file was successfully uploaded.

    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = Path(file_name).name

    # Finds access keys for user 'ilya', the first non-header row in the
    # access keys CSV.
    with open('access_keys.csv') as f:
        reader = csv.reader(f)
        next(reader)
        access_id, access_key = next(reader)

    # Opens a connection to the AWS S3 instance.
    s3_client = boto3.resource(
        's3', aws_access_key_id=access_id, aws_secret_access_key=access_key
    )

    # Finds a particular bucket.
    bucket = s3_client.Bucket(bucket)

    # Uploads the file to the bucket.
    try:
        bucket.upload_file(file_name, object_name)
    except ClientError:
        return False

    return True


check_data(generate_data()).to_parquet("/tmp/test_data.parquet")
bucket_push("/tmp/test_data.parquet", "byte-camp-person-data", "test_data")
