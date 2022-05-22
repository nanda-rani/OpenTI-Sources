#ip,date,source
import re
import requests
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor

global file
file = 'origdatabase_ip.csv'

global today_date
today_date = datetime.date.today().strftime('%y%m%d')

def csvreader():
	dataf = pd.read_csv(file)
	return dataf

def download_file(url):
	try:
		html = requests.get(url,timeout=5)
		print(f'working on {url}')
		if html.status_code == 200:
			print(f'{url} data downloaded')
			return html
		else:
			print(f'Error code {html.status_code} for {url}')
	except:
		print(f'{url} error')


def add_to_set(data):
	try:
		for line in data.iter_lines():
			k = re.search(r'((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)', line.decode())
			if k != None:
				set1.add(k[0])
		return set1
	except:
		pass

def csvwriter(set1,df,url):
	l = len(set1)
	list1 = list(set1)
	list3 = [url for i in range(l)]
	list2 = [today_date for i in range(l)]
	fd = pd.DataFrame(list(zip(list1,list2,list3)),columns=['ip','date','source'])
	result = pd.concat([df,fd],ignore_index=True)
	result = result.drop_duplicates('ip',keep='first')
	return result



if __name__ == "__main__":
	print('[+] Script Started')
	startt = datetime.datetime.now()
	url_list = open('urls2.txt', 'r').read().strip().split()
	urls = list(filter(lambda x: x[0] != '#', url_list))
	print(len(urls))
	processes = []
	

	with ThreadPoolExecutor(max_workers=8) as executor:
		for url in urls:
			processes.append(executor.submit(download_file, url))
	
	csvdata = csvreader()
	set1 = set()
	orig_data_len = len(csvdata.index)
	print('[+] data acquired from online processing it')
	for task in as_completed(processes):
		try:
			set1 = add_to_set(task.result())
			csvdata = csvwriter(set1,csvdata,task.result().url)
			set1.clear()
			print(f'{task.result().url} is done')
		except:
			continue


	csvdata.to_csv(file,index=False)
	endt = datetime.datetime.now()
	print(f'[+] Total IPs in the database is {len(csvdata.index)}')
	print(f'[+] Total NEW IPs added in the database {len(csvdata.index)-orig_data_len}')
	print(f'[+] Total time taken for execution {str(endt-startt)[2:4]} mins : {str(endt-startt)[5:]} secs')
