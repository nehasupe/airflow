import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from smart_open import open
import pandas as pd
from google.cloud import storage

class WriteCSVFile(beam.DoFn):
	def __init__(self, bucket_name):
		self.bucket_name = bucket_name

	def start_bundle(self):
		self.client = storage.Client()

	def process(self, mylist):
		df = pd.DataFrame(mylist, columns={'product_id': str, 'product_name': str, 'product_price': str, 'currency': currency, 'created_at': str})
		bucket = self.client.get_bucket(self.bucket_name)
		bucket.blob(f'csv_exports.csv').upload_from_string(df.to_csv(index=False), 'text/csv')

