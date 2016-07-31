import requests

def get_session(user_agent):
	session = requests.Session()
	session.headers["User-Agent"] = user_agent
	return session