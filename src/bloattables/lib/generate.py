import csv
import datetime
import importlib.metadata
from io import StringIO
from pathlib import Path
import random
from typing import TypeVar

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import pandera as pa
import numpy

import bloattables.lib.access as access


__version__: str = importlib.metadata.version('bloattables')


_T = TypeVar('_T')


def load_data(*args: str | Path) -> list[str]:
    """Takes the individual lines within a text file and returns the
    contents as a list.

    Args:
        args: The elements constructing the path to the text file.

    Returns:
        A list of string values taken from the text file.
    """
    # Finds the root file where the source code for bloattables is
    # stored.
    root = Path(__file__).parent.parent

    # Obtains the contents of the file.
    with open(root / 'assets' / Path(*args)) as f:
        contents = f.read().splitlines()

    return contents


def sample_triangular(items: list[_T]) -> _T:
    """Uses a list of items and samples from a triangular sample where
    the mode is the first index.

    Args:
        items: The items from which to be sampled.

    Returns:
        The result from the sample.
    """
    index = int(numpy.random.triangular(left=0, mode=0, right=len(items)))
    return items[index]


def generate_data(quantity: int = 1_000) -> pd.DataFrame:
    """Generates a large quantity of test data with person details.

    Args:
        quantity: The number of rows of data to be generated.

    Returns:
        A pandas DataFrame with all the test data.
    """
    # Prepares to store a CSV file in memory.
    output = StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(['person_id', 'fname', 'lname', 'sex', 'date_of_birth'])

    # Loads each list of names.
    male_fnames = load_data('fnames', 'male')
    female_fnames = load_data('fnames', 'female')
    lnames = load_data('lnames', 'lnames')

    # Finds the time today.
    today = pd.Timestamp.today()

    # Adds new rows of data to the CSV.
    for no_row in range(quantity):
        # Obtains the values to be written to the CSV.
        df_id = no_row + 1
        sex = random.choice(('male', 'female'))
        fname = sample_triangular(male_fnames if sex == 'male' else female_fnames)
        lname = sample_triangular(lnames)
        days_old = random.randint(a=1, b=32850)
        date_of_birth = today - datetime.timedelta(days=days_old)

        # Writes a row of data to the CSV.
        csv_writer.writerow([df_id, fname, lname, sex, date_of_birth])

    # Goes back to the start of the CSV instance for reading.
    output.seek(0)

    # Returns the CSV contents as a dataframe.
    return pd.read_csv(output, parse_dates=['date_of_birth'])


def check_data(df: pd.DataFrame) -> pd.DataFrame:
    """Verifies that the test data is usable and contains no unexpected
    values, raising an exception if not.

    Args:
        df: The test data.

    Returns:
        The verified data frame.
    """
    # Determines the two dates between which it is expected that the
    # date of birth is found.
    today = pd.Timestamp.today()
    earliest_permissible_dob = pd.Timestamp('1920-01-01')

    # Creates a schema to validate the provided data.
    schema = pa.DataFrameSchema({
        'person_id': pa.Column(int, pa.Check.greater_than(0)),
        'fname': pa.Column(str),
        'lname': pa.Column(str),
        'sex': pa.Column(str, checks=[pa.Check.str_matches('male|female')]),
        'date_of_birth': pa.Column(pa.dtypes.DateTime, pa.Check(
            lambda dt: earliest_permissible_dob <= dt <= today, element_wise=True
        ))
    })

    # Returns the validated data.
    return schema.validate(df)


def bucket_push(file_name: str | Path, bucket: str, object_name: str | None = None) -> bool:
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
    access_id, access_key = access.credentials()

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


def create_parquet(
        location: str | Path,
        *,
        quantity: int = 1_000
):
    """Creates a parquet file with test data and saves it to a specified
    location.

    Args:
        location: The path to where the parquet file must be saved.
        quantity: The number of rows to be saved in the file.
    """
    data = generate_data(quantity)
    check_data(data)
    data.to_parquet(location)
