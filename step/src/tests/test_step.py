import os

import boto3
import moto


@moto.mock_s3()
def test_get_object():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    # TODO put proper avro data
    s3.put_object(Bucket='test-bucket', Key='test-key', Body=b'test-body')

    os.environ['URL'] = 'http://localhost:42'
    from ..step import get_object

    it = get_object(bucket='test-bucket', key='test-key')
    assert len(list(it)) == 1
