from tasks import PipelineTransform

import unittest

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromCsv
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

options = PipelineOptions(runner = 'DirectRunner')

class TestPipelineTransform(unittest.TestCase):
    def runTest(self):
        expected_output = [
            {"date": "2022-10-14", "transaction_amount": 59.870000000000005},
            {"date": "2015-12-25", "transcation_amount": 9999.99},
            {"date": "2033-01-12", "transcation_amount": 40}
        ]

        with TestPipeline(options=options) as p: 
            transformations = (
                p
                | ReadFromCsv("test/test_data.csv")
                | "Apply Transformations" >> PipelineTransform()
            )

        p.run()
    
        assert_that(transformations, equal_to(expected_output))
