import argparse
import datetime
import logging
import pandas as pd
import typing
from typing import Optional


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.convert import to_dataframe, to_pcollection


class Schema(typing.NamedTuple):
    """Class needed to enforce schema on PCollection."""
    timestamp: datetime.datetime 
    origin: str
    destination: str
    transaction_amount: float


class ConvertToSchemaPCollection(beam.PTransform):
    """PTransform which applies Schema class to PCollection."""
    def expand(self, raw_data: beam.PCollection) -> beam.PCollection:
        """Make each of PCollection a list of strings, and apply Schema to PCollection.
        
        :param self: Instance of ConvertToSchemaPCollection class.
        :param raw_data: Pcollection of raw data from input.
        """
        return (
            raw_data
            | beam.Map(lambda x: x.split(","))
            | beam.Map(lambda x: Schema(*x))
        )


class DataFrameTransforms(beam.PTransform):
    """PTransform which applies all transformations needed on Schema'd PCollection."""
    def expand(self, schema_pcoll: beam.PCollection) -> beam.PCollection:
        """Convert PCollection to Deffered DataFrame, transform data through Beam Pandas API, and convert back to PCollection.
        
        :param self: Instance of DataFrameTransforms class.
        :param schema_pcoll: Schema'd PCollection.
        """
        df = to_dataframe(schema_pcoll)
        df = df[df["transaction_amount"] > 20]
        df["date"] = df["timestamp"].apply(lambda x: pd.to_datetime(x).date())
        df = df[df["date"] > datetime.date(2009, 12, 31)]
        df = df.groupby("date").sum()
        df.to_csv("output/results.csv")
        return to_pcollection(df, include_indexes=True)


class CompositeTransform(beam.PTransform):
    """PTransform which runs all transforms of the pipeline, besides the IO read transform."""
    def expand(self, raw_data: beam.PCollection) -> beam.PCollection:
        """Run ConvertToSchemaPCollection followed by DataFrameTransforms.

        :param raw_data: Pcollection of raw data from input.
        """
        return (
            raw_data
            | ConvertToSchemaPCollection()
            | DataFrameTransforms()
        )

def run(argv: Optional[argparse.ArgumentParser] = None):
    """Run pipeline, which entails following 2 steps: read input data, run CompositeTransform.


    :param argv: Optional argparse.ArgumentParser object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | CompositeTransform()
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
