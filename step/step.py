import io
import os
from collections.abc import Iterator
from typing import Any


import boto3
from avro.datafile import DataFileReader
from avro.io import DatumReader
from elasticsearch import Elasticsearch
from elasticsearch import helpers


es = Elasticsearch(os.environ['URL'])
s3 = boto3.client('s3')


def get_object(*, bucket: str, key: str) -> Iterator[Any]:
    response = s3.get_object(Bucket=bucket, Key=key)
    reader = io.BufferedReader(response['Body']._raw_stream)
    return DataFileReader(reader, DatumReader())


def ingest_into_es(index: str, iterator):
    docs = (
        {
            '_index': index,
            '_id': msg.get('id'),
            '_source': msg,
        } for msg in iterator
    )
    helpers.bulk(es, docs)


def handler(event, context):
    # index = event['index']
    pass
