"""
Logging facade for the CDLS.

Attributes:
  _DEFAULT_BANNER_WIDTH (int): Banner will be this number of characters wide.
  _DEFAULT_STACK_DEPTH (int): Controls how deep to look into the stack for the
    name of the invoking function.
  _TEMPLATE (string): The template for each normal log entry.
  _OUTFILE  (string): The file being written to.

"""

import datetime
import inspect
import os.path
import traceback

import cdls.config

_DEFAULT_BANNER_WIDTH = 80
_DEFAULT_STACK_DEPTH = 3
_TEMPLATE = cdls.config.LOGGING_FORMAT
_OUTFILE  = os.path.join(cdls.config.LOGGING_DIRECTORY,
	                     "{:%Y-%m}.log".format(datetime.date.today()))


def banner(level, message, *args, **kwargs):
	"""
	Format and a single log message into a banner-style log message

	Args:
	  level (string): The logging level
	  message (string): The message to be logged
	  *args (list, optional): Any formatted arguments for the message
	  **kwargs (dict, optional): Any banner-formatting flags.
	    `width` allows the caller to specify how wide the banner should be.

	Returns:
	  string: The formatted log message

	"""

	width = kwargs.get("width", _DEFAULT_BANNER_WIDTH)

	hr = "*" * width
	emptyline = "*" + (" " * (width - 2)) + "*"
	line_template = "* {:^" +  str(width - 4) + "} *"

	# Top portion
	_log_entry(level, hr, **kwargs)
	_log_entry(level, emptyline, **kwargs)

	# Handle any message formatting beforehand
	message = message.format(*args)

	# Break long statement into lines
	buf = ""
	for word in message.split():
		if len(buf) + len(word) + 1 <= width - 4:
			buf += " " + word.strip()
		else:
			_log_entry(level, line_template, buf.strip(), **kwargs)
			buf = word

	# Flush anything that might still be in the buffer
	if buf:
		_log_entry(level, line_template, buf.strip(), **kwargs)

	# Bottom portion
	_log_entry(level, emptyline, **kwargs)
	_log_entry(level, hr, **kwargs)


def info(message, *args, **kwargs):
	"""Logs an INFO-level message.

	Args:
	  message (string): The message to be logged
	  *args (dict, optional): Any items to include in the formatting
	  **kwargs (dict, optional): Any context flags (refer to _log_entry for
	    more flag info).
	
	Returns:
	  string: The line that was logged

	"""
	return _log_entry("info", message, *args, **kwargs)


def error(message, *args, **kwargs):
	"""Logs an ERROR-level message

	Args:
	  message (string): The message to be logged
	  *args (dict, optional): Any items to include in the formatting
	  **kwargs (dict, optional): Any context flags (refer to _log_entry for
	    more flag info).

	Returns:
	  string: The line that was logged

	"""
	return _log_entry("error", message, *args, **kwargs)


def warn(message, *args, **kwargs):
	"""Logs a WARN-level message.

	Args:
	  message (string): The message to be logged
	  *args (dict, optional): Any items to include in the formatting
	  **kwargs (dict, optional): Any context flags (refer to _log_entry for
	    more flag info).

	Returns:
	  string: The line that was logged

	"""
	return _log_entry("warn", message, *args, **kwargs)


def exception(exception):
	"""Prints a formatted exception log entry.

	Args:
	  exception (CDLSException): The exception to be logged

	Returns:
	  string: The formatted exception if noisy-flag is on, otherwise an
	    ERROR-level message.

	"""

	template = """!!!!!!!!!!!!!!!!!!!!!!!!!!!! {0} !!!!!!!!!!!!!!!!!!!!!!!!!!!

>> {1}

Message:
{2}

Stack Trace:
{3}

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"""

	exception_type = type(exception).__name__
	timestamp      = _generate_timestamp()
	message        = str(exception)
	stack_trace    = traceback.format_exc()

	# Check the exception for extra details
	try:
		details = exception.details()
		if details is not None:
			message += "\n\n" + details
	except AttributeError:
		pass

	# Assemble the formatted exception log
	output = template.format(timestamp, exception_type, message, stack_trace).strip()

	# Write to log file
	_append_to_logfile(output)

	# Directly access LOGGING_NOISY setting because it may change at runtime
	if cdls.config.LOGGING_NOISY:
		print(output)

	return _log_entry("error", str(exception))


def _append_to_logfile(message):
	"""Adds a new log line to the log file."""
	with open(_OUTFILE, "a") as fp:
		fp.write(message + "\n")


def _generate_timestamp():
	"""Returns a formatted timestamp string."""
	return datetime.datetime.now().strftime("%m/%d %H:%M:%S,%f")


def _get_invoking_method_name(depth):
	"""Gets the name of the invoking method.

	Args:
	  depth (int): The depth in the call stack to peek

	Returns:
	  string

	"""
	return inspect.stack()[depth][3]


def _log_entry(level, message, *args, **kwargs):
	"""
	Format and log a single formatted message

	Args:
	  level (string): The logging level
	  message (string): The message to be logged
	  *args (dict, optional): Any items to include in the formatting
	  **kwargs (dict, optional): Any context flags.

	    `degree` helps find the right invoking method by offsetting how deep
	    we're digging into the execution stack.

	    `context` allows the caller to replace the context outright with an
	    arbitrary string.

	    `tag` allows user to prepend a string in front of the context, such as
	    a unique identifier.

	Returns:
	  string: The formatted log message

	"""

	# Tag the message with the context name
	stack_depth = _DEFAULT_STACK_DEPTH
	stack_depth += kwargs.get("degree", 0)
	context = kwargs.get("context", _get_invoking_method_name(stack_depth))

	# Add any other pertinent tags to the context
	tag = kwargs.get("tag")
	if tag:
		context = "{}.{}".format(tag, context)

	# Construct the complete message
	message = "[{0}:] {1}".format(context, message.strip().format(*args))

	# Format the entire line
	output = _TEMPLATE.format(timestamp = _generate_timestamp(),
		                          level = level.strip().upper(),
		                        message = message).strip()

	# Emit to STDOUT and file
	print(output)
	_append_to_logfile(output)

	return output
