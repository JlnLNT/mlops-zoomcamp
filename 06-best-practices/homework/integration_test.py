#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import pandas as pd
import batch
import os


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)


data = [
    (None, None, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
    (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
]

columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]
df_input = pd.DataFrame(data, columns=columns)


path = batch.get_input_path(2021, 1)


s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")

options = {"client_kwargs": {"endpoint_url": s3_endpoint_url}}


df_input.to_parquet(
    path,
    engine="pyarrow",
    compression=None,
    index=False,
    storage_options=options,
)
