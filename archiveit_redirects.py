from scripts.config import get_org_code
from scripts.redirects import get_redirect_metadata

import os
from os.path import join
import sys

def archiveit_redirects(job, jobs_dir, config_file):
	job_dir = join(jobs_dir, job)
	if not os.path.exists(job_dir):
		print "Directory for job {} not found at {}".format(job, job_dir)
		sys.exit()

	print "Getting redirect metadata for {}".format(job)
	org_code = get_org_code(config_file)
	get_redirect_metadata(job_dir, org_code)

if __name__ == "__main__":
	base_dir = os.path.dirname(os.path.abspath(__file__))
	jobs_dir = join(base_dir, "jobs")
	config_file = join(base_dir, "config.txt")

	job_numbers = raw_input("Enter a comma separated list of job numbers: ")
	jobs = [job.strip() for job in job_numbers.split(",")]
	for job in jobs:
		archiveit_redirects(job, jobs_dir, config_file)