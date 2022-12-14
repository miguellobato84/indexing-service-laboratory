import os

import boto3
import moto
from unittest.mock import ANY


dir_path = os.path.dirname(os.path.realpath(__file__))

# setup environ variable before import
os.environ['URL'] = 'http://localhost:42'


@moto.mock_s3()
def test_get_object_decodes_avro(mocker):
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket')
    with open(os.path.join(dir_path, "data", "simple.avro"), 'rb') as f:
        s3.put_object(Bucket='test-bucket', Key='simple.avro', Body=f)

    mocker.patch('step.index.Elasticsearch')
    from step.index import get_object

    it = get_object(bucket='test-bucket', key='simple.avro')

    result_list = list(it)
    assert len(result_list) == 1
    assert {"first": "Donald", "last": "Duck"} == result_list[0]


def test_ingest_into_es(mocker):
    es_helpers_mock = mocker.patch('step.index.helpers')
    mocker.patch('step.index.Elasticsearch')
    from step.index import ingest_into_es, es

    ingest_into_es('test-index', [{'content': 'test-message'}].__iter__())

    es_helpers_mock.bulk.assert_called_with(es, ANY)
