import argparse
import json
import logging
import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
from readjson import ReadFile
from writecsv import WriteCSVFile

class DataflowOptions(PipelineOptions):
	@classmethod
	def _add_argparse_args(cls, parser):
		parser.add_argument('--input_path', type=str, default='gs://dataflow_temp_n/products.json')
		parser.add_argument('--output_bucket', type=str, default='gs://dataflow_temp_n')

def run(argv=None):
	parser = argparse.ArgumentParser()
	known_args, pipeline_args = parser.parse_known_args(argv)

	pipeline_options = PipelineOptions(pipeline_args)
	dataflow_options = pipeline_options.view_as(DataflowOptions)

	with beam.Pipeline(options=pipeline_options) as pipeline:
		(pipeline | 'start' >> beam.Create([None]) | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path)) | 'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket)))

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	run()
