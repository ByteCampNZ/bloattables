from pathlib import Path

from bloattables.lib.generate import create_parquet, upload_to_google_cloud

if __name__ == "__main__":
    location = Path("/tmp/test_data.parquet")
    create_parquet(location=location, quantity=10)
    upload_to_google_cloud(path_to_file=location)
