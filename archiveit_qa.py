from scripts.config import get_user_agent
from scripts.config import save_config
from scripts.make_dirs import make_directories
from scripts.reports import get_base_reports, get_host_info, get_seed_source, get_crawl_reports, extract_reports
from scripts.repeats import find_repeat_dirs, check_repeat_url_status, process_repeats
from scripts.redirects import check_seed_status, minimal_redirect_handling
from scripts.get_session import get_session

import ConfigParser
import os
from os.path import join
import sys

def run_archiveit_qa(job, jobs_dir, config_file):
	user_agent = get_user_agent(config_file)
	session = get_session(user_agent)
	job_dir = join(jobs_dir, job)
	make_directories(job_dir)
	get_base_reports(job, job_dir, session)
	host_dict, host_list = get_host_info(job_dir)
	get_crawl_reports(job_dir, job, host_dict, session)
	find_repeat_dirs(job_dir, host_list, session)
	source_list = get_seed_source(job_dir)
	check_seed_status(job_dir, source_list)
	session.close()

def main():
	base_dir = os.path.dirname(os.path.abspath(__file__))

	config_file = join(base_dir, "config.txt")
	if not os.path.exists(config_file):
		save_config(config_file)
		
	jobs_dir = join(base_dir, "jobs")
	if not os.path.exists(jobs_dir):
		os.makedirs(jobs_dir)

	job_numbers = raw_input("Enter a comma separated list of job numbers: ")
	jobs = [job.strip() for job in job_numbers.split(",")]
	for job in jobs:
		run_archiveit_qa(job, jobs_dir, config_file)

if __name__ == "__main__":
	main()
