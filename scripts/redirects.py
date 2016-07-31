import csv
import HTMLParser
from lxml import etree
import os
from os.path import join
import re
import requests
import urlparse

def get_collection_id(job_dir):
	seedstatus_csv = join(job_dir, "seedstatus.csv")
	with open(seedstatus_csv, "rb") as csvfile:
		reader = csv.reader(csvfile)
		first_row = reader.next()
		collection_string = first_row[0]
		collection_id = re.findall(r"(\d+)\t", collection_string)[0]

	return collection_id

def build_redirect_dict(job_dir):
	redirect_dict = {}

	redirect_dir = join(job_dir, "redirects")
	redirect_dir = join(job_dir, "redirects")
	redirect_csv = join(redirect_dir, "redirect_information.csv")	
	with open(redirect_csv, "rb") as redirect_csv:
		reader = csv.reader(redirect_csv)
		next(reader, None)
		for row in reader:
			seed = row[0].strip()
			redirect = row[1].strip()
			redirect_dict[seed] = redirect

	return redirect_dict

def get_redirect_metadata(job_dir, org_code):
	redirect_dict = build_redirect_dict(job_dir)
	collection_id = get_collection_id(job_dir)

	redirect_dir = join(job_dir, "redirects")
	add_deactivate_csv = join(redirect_dir, "add_and_deactivate.csv")
	redirect_investigate_csv = join(redirect_dir, "redirect_investigate.csv")
	redirect_metadata_csv = join(redirect_dir, "redirect_metadata.csv")

	skip = ['createdDate','lastUpdatedDate','active','public','note','url']
	starting_seeds = {}
	for seed in redirect_dict:
		starting_seeds[seed] = ""
	with requests.Session() as s:
		collection_feed = s.get("https://partner.archive-it.org/seam/resource/collectionFeed?accountId={0}&collectionId={1}".format(org_code, collection_id))
	collection_metadata = etree.fromstring(collection_feed.text.encode("utf-8"))
	tree = etree.ElementTree(collection_metadata)
	seeds = tree.xpath("//seed")
	for seed in seeds:
		url = seed.xpath("./url")[0].text
		if url in starting_seeds:
			starting_seeds[url] = tree.getpath(seed)

	redirect_metadata = []
	add_deactivate = {}
	redirect_investigate = {}
	entity_parser = HTMLParser.HTMLParser()

	for seed in starting_seeds.keys():
		if len(starting_seeds[seed]) > 0:
			new_seed = redirect_dict[seed]
			add_deactivate[seed] = new_seed
			seed_metadata = {}
			seed_path = starting_seeds[seed]
			seed_element = tree.xpath(seed_path)[0]
			for elem in seed_element.xpath(".//*"):
				if elem.text is not None and not elem.tag in skip and not "name" in elem.attrib:
					elem_name = elem.tag
					elem_text = entity_parser.unescape(elem.text.replace('&#8220;','"').replace('&#8221;','"').replace('&#8217;',"'"))
				elif "name" in elem.attrib and elem.attrib["name"] not in skip:
					elem_name = elem.attrib["name"]
					elem_text = entity_parser.unescape(elem.text.replace('&#8220;','"').replace('&#8221;','"').replace('&#8217;',"'"))
				else:
					elem_name = False
					elem_text = False
				if elem_name and elem_text:
					if elem_name not in seed_metadata:
						seed_metadata[elem_name] = []
					seed_metadata[elem_name].append(elem_text.encode("utf-8"))
			seed_metadata["url"] = [new_seed]
			seed_metadata["Note"] = ["QA NOTE: This seed was created as a result of the previous seed URL redirecting to this URL. Previous captures under seed URL {}".format(seed)]
			redirect_metadata.append(seed_metadata)
		else:
			redirect_investigate[seed] = redirect_dict[seed]

	add_deactivate_data = [[new_seed, seed, "QA NOTE: Seed deactivated. Seed URL redirects to {}. A new seed with the redirected seed URL has been added.".format(new_seed)] for seed, new_seed in add_deactivate.items()]
	with open(add_deactivate_csv, "wb") as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(["Add", "Deactivate", "Deactivation Note"])
		writer.writerows(add_deactivate_data)

	if len(redirect_investigate) > 0:
		redirect_investigate_data = [[seed, new_seed] for seed, new_seed in redirect_investigate.items()]
		with open(redirect_investigate_csv, "wb") as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow(["Seed URL", "Redirect URL"])
			writer.writerows(redirect_investigate_data)

	header_order = ['url','Title','Subject','Personal Creator','Corporate Creator','Coverage','Description','Publisher','Note']
	header_counts = {}
	for seed in redirect_metadata:
		for element in seed:
			count = len(seed[element])
			if element not in header_counts:
				header_counts[element] = count
			elif count > header_counts[element]:
				header_counts[element] = count
	for element in header_order:
		if element not in header_counts and element.lower() not in header_counts:
			header_counts[element] = 1
	for seed in redirect_metadata:
		for element in header_counts:
			if element not in seed:
				seed[element] = []
		for element in seed:
			current_count = len(seed[element])
			header_count = header_counts[element]
			difference = header_count - current_count
			if difference > 0:
				seed[element].extend([""] * difference)
	header_row = []
	header_counts_lower = {k.lower():v for k, v in header_counts.items()}
	for element in header_order:
		elem_lower = element.lower()
		header_row.extend([element] * header_counts_lower[elem_lower])
	redirect_csv_metadata = []
	for seed in redirect_metadata:
		row = []
		for element in header_order:
			elem_lower = element.lower()
			if element in seed:
				row.extend([item for item in seed[element]])
			elif elem_lower in seed:
				row.extend([item for item in seed[elem_lower]])
		redirect_csv_metadata.append(row)
	with open(redirect_metadata_csv, "wb") as csvfile:
		writer = csv.writer(csvfile)
		writer.writerow(header_row)
		writer.writerows(redirect_csv_metadata)

def minimal_redirect_handling(job_dir, source_list, redirects):
	print "Redirects found! Doing some minimal redirect handling"
	redirect_dir = join(job_dir, "redirects")
	starting_seeds = {}
	reconciled = False
	while not reconciled:
		unreconciled_count = 0
		for url in redirects:
			value = redirects[url]
			if (url in source_list) and (value in redirects):
				unreconciled_count += 1
				redirects[url] = redirects[value]
			if unreconciled_count == 0:
				reconciled = True
	for url in redirects:
		if url in source_list:
			redirect_url = redirects[url]
			seed_parse = urlparse.urlparse(url)
			redirect_parse = urlparse.urlparse(redirect_url)
			if ((seed_parse.path != redirect_parse.path) and ((seed_parse.path + '/' != redirect_parse.path) and (seed_parse.path != redirect_parse.path + '/'))) or ((seed_parse.netloc != redirect_parse.netloc) and (('www.' + seed_parse.netloc != redirect_parse.netloc) and (seed_parse.netloc != 'www.' + redirect_parse.netloc))) or (seed_parse.params != redirect_parse.params) or (seed_parse.query != redirect_parse.query) or (seed_parse.fragment != redirect_parse.fragment):
				starting_seeds[url] = redirect_url

	redirect_data = [[seed, redirect, ""] for seed, redirect in starting_seeds.items()]
	redirect_csv = join(redirect_dir, "redirect_information.csv")
	with open(redirect_csv, "wb") as f:
		writer = csv.writer(f)
		writer.writerow(["Seed URL", "Redirect URL", "Notes"])
		writer.writerows(redirect_data)

def check_seed_status(job_dir, source_list):
	print "Checking seed status"
	statuses = {"redirects":{},"robots":{},"unknown":{},"ok":{}}
	status_dir = join(job_dir, "statuses")
	status_csv = join(job_dir, "seedstatus.csv")
	with open(status_csv, "rb") as f:
		reader = csv.reader(f)
		next(reader, None)
		next(reader, None)
		for row in reader:
			status = row[0]
			url = row[1]
			code = row[2]
			redirect = row[3]
			if code != "200":
				if code in ["301", "302"]:
					statuses["redirects"][url] = redirect
				elif code == "-9998":
					statuses["robots"][url] = code
				else:
					statuses["unknown"][url] = code
			else:
				statuses["ok"][url] = code

	for status in statuses:
		status_csv = join(status_dir, "{}.csv".format(status))
		status_data = [[url, status] for url, status in statuses[status].items()]
		with open(status_csv, "wb") as f:
			writer = csv.writer(f)
			writer.writerows(status_data)

	redirects = statuses["redirects"]
	if len(redirects) > 0:
		if not os.path.exists(join(job_dir, "redirects")):
			os.makedirs(join(job_dir, "redirects"))
		minimal_redirect_handling(job_dir, source_list, redirects)


