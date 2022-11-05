#!/usr/bin/env python
# coding: utf-8

from datetime import datetime
import pandas as pd

import batch


def test_prep():
    def dt(hour, minute, second=0):
        return datetime(2021, 1, 1, hour, minute, second)

    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),
    ]

    columns = ["PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime"]
    df = pd.DataFrame(data, columns=columns)

    categorical = ["PUlocationID", "DOlocationID"]
    df_out = batch.prepare_data(df, categorical)

    expected_data = [
        ("-1", "-1", dt(1, 2), dt(1, 10), 8),
        ("1", "1", dt(1, 2), dt(1, 10), 8),
    ]
    expected_df = pd.DataFrame(expected_data, columns=columns + ["duration"])

    assert expected_df.to_dict() == df_out.to_dict()
