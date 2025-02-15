from bloattables.lib.generate import bucket_push, create_parquet
# from bloattables.lib.fetch import fetch


if __name__ == '__main__':
    create_parquet(location='/tmp/test_data.parquet')
    bucket_push('/tmp/test_data.parquet', 'byte-camp-person-data', 'test_data')
    # results = fetch('byte-camp-person-data', 'ap-southeast-2', 'test_data')
    # print(results)
