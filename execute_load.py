import cdls
import optparse

from cdls.errors import CDLSError

parser = None

def func_doc(func):
	output = []
	for line in func.__doc__.split("\n"):
		line = line.strip()
		if line:
			output.append(line)
		else:
			return " ".join(output)


def main():
	global parser

	# Initialize options parser
	parser = optparse.OptionParser()
	parser.add_option("-l", "--list", action="store_true", help=func_doc(cdls.list_registered_sources))
	parser.add_option("-a", "--all", action="store_true", help=func_doc(cdls.perform_all_loads))
	parser.add_option("-n", "--noisy", action="store_true", help="Outputs more verbose logging info")
	parser.add_option("-i", "--install-db", action="store_true", help="Installs the database schema")
	(options, args) = parser.parse_args(

		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG

		# ["-n", "-a"] #all
		# ["-n", "local0", "local1"] # only local0
		# ["-l"] # list all sources

		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG
		# DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG DEBUG

		)

	# print("options:", options)
	# print("args:", args)
	# exit()

	# Perform actions based on the user's CLI options
	if args or options.list or options.all or options.install_db:

		# Adjust log noisiness as required
		if options.noisy:
			cdls.config.LOGGING_NOISY = True

		# Bring the whole CDLS up
		initialize()

		# Choose to rebuild the schema
		if options.install_db:
			install()

		# Load one or multiple sources
		if options.list and not options.all and not args:
			list_all_sources()
		elif args and not options.list and not options.all:
			for identifier in args:
				load_source(identifier)
		elif options.all and not options.list and not args:
			load_all_sources()
		else:
			# Only valid combinations are listed above
			return handle_error_invalid_combination()

	else:
		return handle_error_no_arguments()


def initialize():
	try:
		cdls.initialize()
	except CDLSError as e:
		return handle_error_initialization_failed(e)


def handle_error_no_arguments():
	parser.print_help()
	exit(1)

def handle_error_invalid_combination():
	print("Error: Invalid argument combination")
	parser.print_help()
	exit(1)


def install():
	print("Rebuilding database schema...")
	cdls.db.install()


def handle_error_initialization_failed(exception):
	name = type(exception).__name__
	message = str(exception)
	print("CDLS Initialization Failed: {0}: {1}".format(name, message))
	exit(1)


def handle_error_fatal(exception):
	name = type(exception).__name__
	message = str(exception)
	print("Fatal Error: {0}: {1}".format(name, message))
	exit(1)


def list_all_sources():
	try:
		output = """
All Registered Sources:

             Identifier   Description
             ----------   -----------
""".lstrip()

		for source_info in cdls.list_registered_sources():
			output += "{0:>23} : ({1}) {2}\n".format(*source_info)
		
		print(output)

	except cdls.errors.CDLSError as e:
		return handle_error_fatal(e)


def load_source(identifier):
	try:
		cdls.perform_load(identifier)
	except cdls.errors.CDLSError as e:
		return handle_error_fatal(e)


def load_all_sources():
	try:
		cdls.perform_all_loads()
	except cdls.errors.CDLSError as e:
		return handle_error_fatal(e)


if "__main__" == __name__:
	main()
