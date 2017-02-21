import boto3
import time 
import threading
import os
import logging
import subprocess
import traceback


from json import dump, loads, JSONEncoder, JSONDecoder
import pickle


def default_handler(obj):
	if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
		return JSONEncoder.default(obj)
	return {'_python_object': pickle.dumps(obj)}

def as_python_object(dct):
	if '_python_object' in dct:
		return pickle.loads(str(dct['_python_object']))
	return dct



def get_dynamo_connection():

	'''
	Sets up dynamo connection

	'''

	sess = boto3.Session(profile_name='impactlab_admin_jsimcock')
	conn = sess.resource('dynamodb', region_name='us-east-1')
	return conn


def get_s3_connection():
	'''
	Set up s3 connection so we can post to our buckets
	'''

	session = boto3.Session(profile_name='cil')
	s3 = session.resource('s3', endpoint_url='https://griffin-objstore.opensciencedatacloud.org')
	return s3



def run_continuously(interval=60):
	"""
	Use scheduler to run in the background
	"""

	while True:
		schedule.run_pending()
		time.sleep(interval)
		

def get_backup_contents(table_name):
	'''
	Gets contents of tables and creates a json dict of contents for the table and its associated spec table

	'''

	last_evaluated_key = None
	
	conn = get_dynamo_connection()
	table = conn.Table(table_name)
	table_spec = conn.Table(table_name + '.spec')
	
	kwargs = {}


	data = []
	while True:
		response = table.scan(**kwargs)
		for res in response['Items']:
			data.append(res)
		if 'LastEvaluatedKey' in response: 
			kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
		else:
			break




	backup_contents = dict(TableSchema=table.key_schema, 
					  ReadCapacityUnit= table.provisioned_throughput['ReadCapacityUnits'], 
					  WriteCapacityUnit= table.provisioned_throughput['WriteCapacityUnits'],
					  SpecTableSchema=table_spec.key_schema,
					  Items=data)

	return backup_contents


def write_to_json(backup_contents):
	
	filename = 'cil_data_backup_' + time.strftime("%Y%m%d-%H%M%S") + ".json"


	with open(filename, "w+") as f:
	
   
		dump(backup_contents,f, default=default_handler, indent=4)
	
	#print(filename)
	return filename


def post_to_osdc(filename):

	s3 = get_s3_connection()
	
	with open(filename, 'rb') as data:
		s3.Bucket('dynamo_backup').upload_fileobj(data, filename)






def main():
	try:
		time_stamp = time.strftime("%Y%m%d-%H%M%S")
		logging.basicConfig(level=logging.INFO)
		backup_contents = get_backup_contents('cil-data')
		#logging.info('Starting dynamo backup for cil-data for date: {}'.format(time_stamp))
		filename = write_to_json(backup_contents)
		#logging.info('Writing contents to file dynamo_backup.log')
		post_to_osdc(filename)
		#logging.info('Posting new backup of cil-date to osdc')
		#os.remove(filename)
		#logging.info('Deleting local backup tempfile for date{}'.format(time_stamp))

	except Exception:
		logging.error(traceback.format_exc())



if __name__ =='__main__':

	# schedule.every(1).minutes.do(main)
	# run_continuously(interval=60)
	main()
	