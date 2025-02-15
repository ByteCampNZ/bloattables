import csv
import os

import dotenv


def credentials() -> tuple[str, str]:
    """Obtains the credentials for connecting to the access bucket.

    Returns:
        The access ID and access key.
    """
    dotenv.load_dotenv()
    return os.environ.get('ACCESS_KEY_ID'), os.environ.get('SECRET_ACCESS_KEY')
