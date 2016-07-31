import csv
import os
from os.path import join
import re
import requests
import time
import zipfile

def get_base_reports(job, job_dir, session):
	print "Downloading base reports for {}".format(job)
	report_types = ["host", "seed", "source"]
	report_filenames = {"host":"host", "seed":"seedstatus", "source":"seedsource"}
	base_report_url = "https://partner.archive-it.org/seam/resource/report?crawlJobId={}&type=".format(job)
	for report_type in report_types:
		report_filename = report_filenames[report_type]
		report_csv = join(job_dir, "{}.csv".format(report_filename))
		report_url = "{0}{1}".format(base_report_url, report_type)
		report_content = session.get(report_url)
		with open(report_csv, "wb") as f:
			f.write(report_content.content)

def get_host_info(job_dir):
	host_dict = {}
	with open(join(job_dir, "host.csv"), "rb") as host_csv:
		reader = csv.reader(host_csv)
		next(reader, None)
		next(reader, None)
		for row in reader:
			host = row[0]
			crawled = row[1]
			data = row[2]
			queued = row[6]
			if host.endswith(":"):
				host = re.sub(r":$","",host)
			host_dict[host] = {"crawled":int(crawled),
								"queued":int(queued),
								"data":int(data)
								}
	host_list = host_dict.keys()
	return host_dict, host_list

def get_seed_source(job_dir):
	source_csv = join(job_dir, "seedsource.csv")
	source_list = []
	with open(source_csv, "rb") as csvfile:
		reader = csv.reader(csvfile)
		next(reader, None)
		next(reader, None)
		for row in reader:
			source = row[0]
			if source not in source_list:
				source_list.append(source)
	return source_list

def get_crawl_report(job_dir, job, host, report_type, session):
	print "Downloading {0} report for {1}".format(report_type, host)
	zip_dir = join(job_dir, "zips")
	txt_dir = join(job_dir, "crawled_queued")
	url = "https://partner.archive-it.org/seam/resource/{0}ByHost?crawlJobId={1}&host={2}".format(report_type, job, host)
	if url.endswith(":"):
		url = re.sub(r":$", r"%3A", url)
	crawl_report = session.get(url)
	content_type = crawl_report.headers["content-type"]
	if content_type == "application/zip":
		if not os.path.exists(zip_dir):
			os.makedirs(zip_dir)
		output_file = join(zip_dir, "{0}-{1}.zip".format(report_type, host))
		with open(output_file, "wb") as f:
			f.write(crawl_report.content)
	elif content_type.startswith("text/plain"):
		output_file = join(txt_dir, "{0}-{1}.txt".format(report_type, host))
		with open(output_file, "w") as f:
			f.write(crawl_report.content)
	time.sleep(2)

def extract_reports(job_dir):
	zip_dir = join(job_dir, "zips")
	print "Extracting {} zip files".format(len(os.listdir(zip_dir)))
	extract_dir = join(job_dir, "crawled_queued")
	for source_zip in os.listdir(zip_dir):
		with zipfile.ZipFile(join(zip_dir, source_zip)) as zf:
			zf.extractall(extract_dir)

def get_crawl_reports(job_dir, job, host_dict, session):
	print "Downloading crawl reports for {}".format(job)
	for host in host_dict:
		if (host_dict[host]["crawled"] > 25) or (host_dict[host]["data"] > 1000000000):
			get_crawl_report(job_dir, job, host, "crawled", session)
		if host_dict[host]["queued"] > 0:
			get_crawl_report(job_dir, job, host, "queued", session)

	zip_dir = join(job_dir, "zips")
	if os.path.exists(zip_dir):
		extract_reports(job_dir)




