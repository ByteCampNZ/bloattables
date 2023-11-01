from dotenv import load_dotenv
from pyathena import connect
from pyathena.connection import Connection
from pyathena.cursor import Cursor

import bloattables.lib.access as access


def fetch(bucket: str, region_name: str, object_name: str):
    access_id, access_key = access.credentials()
    connection: Connection = connect(
        s3_staging_dir=bucket,
        region_name=region_name,
        aws_access_key_id=access_id,
        aws_secret_access_key=access_key
    )
    cursor = connection.cursor()
    cursor.execute('SELECT TOP 10 * FROM test_data')
    # cursor.execute('SELECT TOP 10 * FROM %(object)', {'object': object_name})
    return cursor.fetchall()
