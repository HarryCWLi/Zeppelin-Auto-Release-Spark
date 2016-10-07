import os
import sys
import json
import datetime
import time
import threading

root_url_list = ['http://ambari-server:9995/api', 'http://127.0.0.1:8080/api']
check_interval = 600
release_overtime = 600

def get_notebook_list(root_url):
	cmd = 'curl -s %s/notebook/' % root_url
	result_dict = json.loads(os.popen(cmd).read().strip())
	
	nb_list = []
	if result_dict['status'] == 'OK':
		for body in result_dict['body']:
			nb = {}
			nb['id'] = body['id']
			nb['name'] = body['name']
			nb_list.append(nb)

	return nb_list
			
def get_last_finish_timestamp(nb_id, root_url):
	cmd = 'curl -s %s/notebook/job/%s' % (root_url, nb_id)
	result_dict = json.loads(os.popen(cmd).read().strip())
	
	ret_ts = 0
	
	if result_dict['status'] == 'OK':
		for job in result_dict['body']:
			if job['status'] == 'RUNNING':
				ret_ts = int(datetime.datetime.now().strftime('%s'))
				break
			elif job['status'] in ['FINISHED', 'ABORT', 'ERROR']:
				new_ts = int(datetime.datetime.strptime(job['finished'], '%a %b %d %H:%M:%S %Z %Y').strftime('%s')) if 'finished' in job else 0
				if new_ts > ret_ts:
					ret_ts = new_ts
	else:
		ret_ts = int(datetime.datetime.now().strftime('%s'))
		
	return ret_ts

def restart_spark_intp(root_url):
	cmd = 'curl -s %s/interpreter/setting' % root_url
	result_dict = json.loads(os.popen(cmd).read().strip())
	if result_dict['status'] == 'OK':
		for body in result_dict['body']:
			if body['name'] == 'spark':
				print 'URL :', root_url
				print 'Found Spark interpreter :', body['id']
				cmd = 'curl -s -X PUT %s/interpreter/setting/restart/%s' % (root_url, body['id'])
				result_dict2 = json.loads(os.popen(cmd).read().strip())
				print 'Restart status :', result_dict2['status']

def process_release(last_release_ts, root_url):
	nb_list = get_notebook_list(root_url)
	
	finished_ts = 0
	for nb in nb_list:
		new_ts = get_last_finish_timestamp(nb['id'], root_url)
		#print nb['id'], nb['name'], datetime.datetime.fromtimestamp(float(new_ts)).strftime('%Y-%m-%d %H:%M:%S')	
		if new_ts > finished_ts:
			finished_ts = new_ts

	print 'URL :', root_url
	print 'Current time :', datetime.datetime.fromtimestamp(float(datetime.datetime.now().strftime('%s'))).strftime('%Y-%m-%d %H:%M:%S')
	print 'Finished time :', datetime.datetime.fromtimestamp(float(finished_ts)).strftime('%Y-%m-%d %H:%M:%S')
	print 'Last release time :', datetime.datetime.fromtimestamp(float(last_release_ts)).strftime('%Y-%m-%d %H:%M:%S')
	
	if finished_ts > last_release_ts:
		now_ts = int(datetime.datetime.now().strftime('%s'))
		if now_ts - finished_ts >= release_overtime:
			restart_spark_intp(root_url)
			return now_ts
		else:
			print 'Not over time.'
			return 0
	else:
		print 'Skip check!'
		return 0

		
def process(root_url):
	print 'Start process for :', root_url
	
	last_release_ts = 0
	while(True):
		release_ts = process_release(last_release_ts, root_url)
		if release_ts > last_release_ts:
			last_release_ts = release_ts
			
		print '=====================\n\n'
		time.sleep(check_interval)
	
if __name__ == '__main__':
	threads = []
	for root_url in root_url_list:
		t = threading.Thread(target = process, args = (root_url,))
		t.setDaemon(True)
		t.start()
		threads.append(t)
		time.sleep(2)
	
	while len(threads) > 0:
		try:
			threads = [t.join(3600) for t in threads if t is not None and t.isAlive()]
				
		except KeyboardInterrupt:
			print 'Receive Ctrl-C'
			for t in threads:
				t.kill_received = True
			break
	