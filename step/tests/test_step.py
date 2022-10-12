import os

import boto3
import moto


dir_path = os.path.dirname(os.path.realpath(__file__))


@moto.mock_s3()
def test_get_object():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    with open(os.path.join(dir_path, "data", "simple.avro"), 'rb') as f:
        s3.put_object(Bucket='test-bucket', Key='simple.avro', Body=f)

    os.environ['URL'] = 'http://localhost:42'
    from step import get_object

    it = get_object(bucket='test-bucket', key='simple.avro')

    result_list = list(it)
    assert len(result_list) == 1
    assert {"first": "Donald", "last": "Duck"} == result_list[0]
