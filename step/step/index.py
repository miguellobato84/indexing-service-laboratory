import os
from collections.abc import Iterator
from typing import Any, Dict


import boto3
from avro.datafile import DataFileReader
from avro.io import DatumReader
from opensearchpy import OpenSearch

from opensearchpy import helpers
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

from urllib import parse

url = parse.urlsplit(os.environ['URL'])

logger = Logger()

es = OpenSearch(
    {
        'host': 'vpc-test-domain-gbboepkscnhmrjphk2mi5ew2d4.eu-central-1.es.amazonaws.com',  # noqa
        'port': 443
    },
    http_auth=('admin', 'admin1234A!'),
    use_ssl=True,
)
s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT_URL'])


def get_object(*, bucket: str, key: str) -> Iterator:
    logger.debug("get_object from s3", extra={'bucket': bucket, 'key': key})

    f = s3.download_file(bucket, key, '/tmp/file.avro')

    logger.debug('initializing avro decoder')
    with open('/tmp/file.avro', 'rb') as f:
        return list(DataFileReader(f, DatumReader()).__iter__()).__iter__()


def ingest_into_es(index: str, iterator: Iterator) -> None:
    docs = (
        {
            '_index': index,
            '_id': msg.get('id'),
            '_source': msg,
        } for msg in iterator
    )
    if not es.indices.exists(index=index):
        es.indices.create(index=index)
    helpers.bulk(es, docs)


@logger.inject_lambda_context(log_event=True)
def handler(event: Dict[str, Any], context: LambdaContext):
    content = get_object(bucket=event['bucket'], key=event['key'])
    ingest_into_es('test', content)
