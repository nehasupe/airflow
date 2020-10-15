import argparse
import json
import logging
import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open
import ast

class ReadFile(beam.DoFn):
	def __init__(self, input_path):
		self.input_path = input_path

	def start_bundle(self):
		self.client = storage.Client()

	def process(self, something):
		clear_data = []
		with open(self.input_path) as fin:
			data = fin.read()
			products_list = ast.literal_eval(data)
			for prod in products_list:
				product_id = prod.get("id")
				product_name = prod.get("name")
				product_price = prod.get("price")
				created_at = prod.get("created_at")
				currency = prod.get("currency")
				clear_data.append([product_id, product_name, product_price, currency, created_at])
		print(clear_data)
		yield clear_data
class WriteCSVFile(beam.DoFn):
	def __init__(self, bucket_name):
		self.bucket_name = bucket_name

	def start_bundle(self):
		self.client = storage.Client()

	def process(self, mylist):
		df = pd.DataFrame(mylist, columns={'product_id': str, 'product_name': str, 'product_price': str, 'currency': currency, 'created_at': str})
		bucket = self.client.get_bucket(self.bucket_name)
		bucket.blob(f'csv_exports.csv').upload_from_string(df.to_csv(index=False), 'text/csv')




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
		(pipeline
		 |'start' >> beam.Create([None]) 
		 |'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path)) 
		 |'Write CSV' >> beam.ParDo(WriteCSVFile(dataflow_options.output_bucket))
		)

if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	run()
