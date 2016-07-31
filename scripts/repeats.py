import csv
import os
from os.path import join
import random
import re

def process_repeats(job_dir, repeat_dict, statuses):
	print "Making repeating directories CSV"
	repeat_dir = join(job_dir, "repeating_directories")
	repeat_csv = join(repeat_dir, "repeating_directories.csv")

	repeat_csv_data = []
	repeat_counts = {host:len(urls) for host, urls in repeat_dict.items()}
	for host in sorted(repeat_counts, key=repeat_counts.get, reverse=True):
		count = repeat_counts[host]
		host_data = [host, count]
		for status in ["ok", "maybe", "notok"]:
			status_list = statuses[host][status]
			status_count = len(status_list)
			host_data.append(status_count)
			if status_count > 0:
				status_dir = join(repeat_dir, status)
				if not os.path.exists(status_dir):
					os.makedirs(status_dir)
				status_file = join(status_dir, "{}.txt".format(host))
				with open(status_file, "w") as f:
					f.write("\n".join(status_list))

		repeat_csv_data.append(host_data)

	with open(repeat_csv, "wb") as f:
		writer = csv.writer(f)
		writer.writerow(["Host", "Count", "OK", "Maybe", "Not OK"])
		writer.writerows(repeat_csv_data)

def check_repeat_url_status(job_dir, repeat_dict, session):
	hosts = repeat_dict.keys()
	total_hosts = len(hosts)
	statuses = {}
	count = 1
	for host in sorted(hosts):
		print "{0}/{1} Checking repeating URL statuses for {2}".format(count, total_hosts, host)
		urls = [url for url in repeat_dict[host]]
		initial_url_count = len(urls)
		statuses[host] = {}
		statuses[host]["ok"] = []
		statuses[host]["maybe"] = []
		statuses[host]["notok"] = []
		repeat_checked = 0
		while (len(urls) > 0) and (repeat_checked < 50):
			urls_to_check = initial_url_count if initial_url_count < 50 else 50
			print "{0}/{1} - {2}".format(repeat_checked+1, urls_to_check, host)
			url = random.choice(urls)
			urls.remove(url)
			try:
				repeat_check = session.head(url)
				if repeat_check.status_code == 200 and len(repeat_check.history) == 0:
					statuses[host]["ok"].append(url)
				elif repeat_check.status_code == 200 and len(repeat_check.history) > 0:
					statuses[host]["maybe"].append(url)
				else:
					statuses[host]["notok"].append(url)
				repeat_checked += 1
				time.sleep(2)
			except:
				continue
		count += 1

	process_repeats(job_dir, repeat_dict, statuses)

def find_repeat_dirs(job_dir, host_list, session):
	print "Checking for repeating directories"
	repeat_dir = join(job_dir, "repeating_directories")
	repeat_dir_all = join(repeat_dir, "all")
	repeat_dirs_string = r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$"
	repeat_dirs_regex = re.compile(repeat_dirs_string)
	repeat_dict = {}
	if os.path.exists(repeat_dir_all):
		print "Loading already identified repeating directories"
		for filename in os.listdir(repeat_dir_all):
			host = filename.replace(".txt","")
			with open(join(repeat_dir_all, filename)) as f:
				urls = f.read().splitlines()
			repeat_dict[host] = urls
	crawl_txts = join(job_dir, "crawled_queued")
	total_txts = len(os.listdir(crawl_txts))
	count = 1
	for filename in os.listdir(crawl_txts):
		print "{0}/{1} Checking for repeating directories".format(count, total_txts)
		hosts = [host for host in host_list if host in filename]
		host = max(hosts, key=len)
		if host not in repeat_dict.keys():
			with open(join(crawl_txts, filename)) as f:
				report = f.read()
			for url in report.splitlines():
				if repeat_dirs_regex.match(url):
					if host not in repeat_dict.keys():
						repeat_dict[host] = []
					repeat_dict[host].append(url)

		count += 1
	if len(repeat_dict) > 0:
		print "Repeating directories found!"
		if not os.path.exists(repeat_dir):
			os.makedirs(repeat_dir)
			os.makedirs(repeat_dir_all)
		regex_file = join(repeat_dir, "repeat_dirs_regex.txt")
		with open(regex_file, "w") as f:
			f.write(repeat_dirs_string)
		for host in repeat_dict:
			file_to_write = join(repeat_dir_all, "{}.txt".format(host))
			if not os.path.exists(file_to_write):
				with open(file_to_write, "w") as f:
					f.write("\n".join(repeat_dict[host]))
		check_repeat_url_status(job_dir, repeat_dict, session)
	else:
		print "No repeating directories found"
		return False


