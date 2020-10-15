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
		yield clear_data
