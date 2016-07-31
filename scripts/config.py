import ConfigParser

def load_config(config_file):
	config = ConfigParser.ConfigParser()
	config.read(config_file)
	return config

def save_config(config_file):
	user_agent = raw_input("Enter your institution's name: ")
	org_code = raw_input("Enter your institution's Archive-It account ID: ")

	config = ConfigParser.ConfigParser()
	config.add_section("main")
	config.set("main","user_agent",user_agent)
	config.set("main","org_code",org_code)
	with open(config_file, "w") as f:
		config.write(f)

def get_user_agent(config_file):
	config = load_config(config_file)
	user_agent = config.get("main", "user_agent")
	return user_agent

def get_org_code(config_file):
	config = load_config(config_file)
	org_code = config.get("main", "org_code")
	return org_code