from pipeline import CompositeTransform

import datetime

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

import unittest

class test_unit_composite_transform(unittest.TestCase):
    def test_output(self):
        test_rows = [
            "2015-10-09 02:54:25 UTC,test_wallet,test_wallet,207.3",
            "2017-01-01 04:22:23 UTC,test_wallet,test_wallet,12.0",
            "2008-03-18 14:09:16 UTC,test_wallet,test_wallet,100.22",
            "1995-12-12 02:54:25 UTC,test_wallet,test_wallet,0",
            "2017-07-09 04:22:23 UTC,test_wallet,test_wallet,20.0",
            "2017-07-09 04:22:23 UTC,test_wallet,test_wallet,20.01",
            "2010-01-01 14:09:16 UTC,test_wallet,test_wallet,1000.0",
            "2009-12-31 14:09:16 UTC,test_wallet,test_wallet,1000.0"
        ]

        with TestPipeline() as p:
            input = p | beam.Create(test_rows)
            output = input | CompositeTransform()

            assert_that(
                output,
                equal_to([
                    (datetime.date(2015, 10, 9), 207.3),
                    (datetime.date(2017, 7, 9), 20.01),
                    (datetime.date(2010, 1, 1), 1000.0)
                ])
            )
