from datetime import datetime
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText

# Set runner to DirectRunner
options = PipelineOptions(runner = 'DirectRunner')


def get_columns(element) -> dict:
    """
    Get the columns required for the transform steps, formatting the data from the CSV
    
    :param dict element: Element
    :return: Dictionary containing selected columns
    :rtype: dict
    """
    date = datetime.strptime(element[0], '%Y-%m-%d %H:%M:%S UTC').date()
    transaction_amount = float(element[3])
    return {"date": date, "transaction_amount": transaction_amount}


def filter_transaction_amount(element: dict, transaction_amount: int) -> dict:
    """
    Filter elements by the transaction_amount

    :param dict element: Element
    :param int transaction_amount: Amount over which should be returned
    :return: Filtered elements
    :rtype: dict
    """
    if element['transaction_amount'] > transaction_amount:
        return element


def filter_year(element: dict, year: int) -> dict:
    """
    Filter elements by year

    :param dict element: Element
    :param int year: Date before which should be excluded
    :return: Filtered elements
    :rtype: dict
    """
    if element['date'].year >= year:
        return element


class PipelineTransform(beam.PTransform):
    """
    Runs the transformations by running the following steps:
    1. Selects required columns from the input CSV, formatting the datatypes of the columns
    2. Filters transaction_amount greater than 20
    3. Exclude transactions made before 2010
    4. Converts to date to string and returns a tuple
    5. Sums the transaction_amount by date
    6. Converts to JSON in preparation for writing to file
    """
    def expand(self, input):
        transformed = (
            input
            | 'Get required columns and format values' >> beam.Map(get_columns)
            | 'Find transactions greater than 20' >> beam.Filter(filter_transaction_amount, 20)
            | 'Exclude transactions made before 2010' >> beam.Filter(filter_year, 2010)
            | 'Convert to tuple' >> beam.Map(lambda cols: (cols['date'].strftime('%Y-%m-%d'), cols['transaction_amount']))
            | 'Sum transaction_amount by date' >> beam.CombinePerKey(sum)
            | 'Convert to JSON' >> beam.Map(lambda cols: json.dumps({"date": cols[0], "transaction_amount": cols[1]}))
        )
        return transformed


def run_pipeline(input_file: str, output_file: str):
    """
    Run the beam pipeline

    :param str input_file: Path to the input CSV file
    :param str output_file: Path to the desination JSONL file
    """
    with beam.Pipeline(options=options) as p:

        # Load CSV, transform, and output the data to jsonl
        transformations = (
            p
            | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=True)
            | 'Split CSV' >> beam.Map(lambda x: x.split(','))
            | 'Run transformations' >> PipelineTransform()
            | 'Write to JSONL' >> WriteToText(f'{output_file}', shard_name_template='', compression_type='gzip')
        )

        p.run()


if __name__ == "__main__":
    # GCS bucket file path and output file name
    input_file = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'
    output_file = 'output/results.jsonl.gz'
    run_pipeline(input_file, output_file)
