import os
from os.path import join

def make_directories(job_dir):
	if not os.path.exists(job_dir):
		print "Making directories"
		os.makedirs(job_dir)
		os.makedirs(join(job_dir, "crawled_queued"))
		os.makedirs(join(job_dir, "statuses"))
