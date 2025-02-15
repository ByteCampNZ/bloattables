import csv
import datetime
import importlib.metadata
import io
import random
from io import StringIO
from pathlib import Path
from typing import TypeVar

import numpy
import pandas as pd
import pandera as pa
from google.cloud import storage
from pyarrow import parquet

__version__: str = importlib.metadata.version("bloattables")


_T = TypeVar("_T")


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
    with open(root / "assets" / Path(*args)) as f:
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
    csv_writer.writerow(["person_id", "fname", "lname", "sex", "date_of_birth"])

    # Loads each list of names.
    male_fnames = load_data("fnames", "male")
    female_fnames = load_data("fnames", "female")
    lnames = load_data("lnames", "lnames")

    # Finds the time today.
    today = pd.Timestamp.today()

    # Adds new rows of data to the CSV.
    for no_row in range(quantity):
        # Obtains the values to be written to the CSV.
        df_id = no_row + 1
        sex = random.choice(("male", "female"))
        fname = sample_triangular(male_fnames if sex == "male" else female_fnames)
        lname = sample_triangular(lnames)
        days_old = random.randint(a=1, b=32850)
        date_of_birth = today - datetime.timedelta(days=days_old)

        # Writes a row of data to the CSV.
        csv_writer.writerow([df_id, fname, lname, sex, date_of_birth])

    # Goes back to the start of the CSV instance for reading.
    output.seek(0)

    # Returns the CSV contents as a dataframe.
    return pd.read_csv(output, parse_dates=["date_of_birth"])


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
    earliest_permissible_dob = pd.Timestamp("1920-01-01")

    # Creates a schema to validate the provided data.
    schema = pa.DataFrameSchema(
        {
            "person_id": pa.Column(int, pa.Check.greater_than(0)),
            "fname": pa.Column(str),
            "lname": pa.Column(str),
            "sex": pa.Column(str, checks=[pa.Check.str_matches("male|female")]),
            "date_of_birth": pa.Column(
                pa.dtypes.DateTime,
                pa.Check(
                    lambda dt: earliest_permissible_dob <= dt <= today,
                    element_wise=True,
                ),
            ),
        }
    )

    # Returns the validated data.
    return schema.validate(df)


def create_parquet(location: str | Path, *, quantity: int = 1_000):
    """Creates a parquet file with test data and saves it to a specified
    location.

    Args:
        location: The path to where the parquet file must be saved.
        quantity: The number of rows to be saved in the file.
    """
    data = generate_data(quantity)
    check_data(data)
    data.to_parquet(location)


def upload_to_google_cloud(path_to_file: Path) -> None:
    table = parquet.read_table(path_to_file)
    parquet_buffer = io.BytesIO()
    parquet.write_table(table, parquet_buffer)
    parquet_bytes = parquet_buffer.getvalue()
    storage_client = storage.Client()
    bucket = storage_client.bucket("bloattables-customers")
    blob = bucket.blob(blob_name="test_data.parquet")
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")
