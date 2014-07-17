"""
Contains all of the supported DataSources for the CDLS

Attributes:
  _REPORT_FORMAT (string): The format for the string representation of a LoadReport object

"""

import datetime
import time

import cdls.config
from cdls.errors import (DatabaseError, ExtractError, SourceConfigurationError, CDLSError)

_REPORT_FORMAT = cdls.config.LOADREPORT_FORMAT

class BaseDataSource:
	"""Abstract representation of a data source, this class provides some basic
	services for subclasses

	Args:
	  config (dict): Configuration parameters for this particular source

	Attributes:
	  _config (dict): The configuration node for this datasource
	  _identifier (string): The unique name for this particular datasource
	  _description (string): A friendly description of this datasource
	  _time_started (float): The time the load operation began
	  _report (LoadReport): A data model for holding load metrics
	  _logger (mixed): Logging facade

	"""
	def __init__(self, config):
		# Saves the config node in case we need it for anything
		self._config = config

		# Grab the basic data from the source config
		self._identifier   = self._get_config_param("id", required=True)
		self._description  = self._get_config_param("description", required=True)

		# Metrics keepers
		self._time_started = float()
		self._report       = LoadReport(self)

		self._db           = None
		self._logger       = None


	def __str__(self):
		return "{0}:{1}".format(self.get_type(), self._identifier)


	def execute(self):
		"""Must be implemented by subclass"""
		raise CDLSError("Not yet implemented")


	def get_identifier(self):
		"""Returns string containing the unique identifier for this datasource"""
		return self._identifier


	def get_type(self):
		"""Returns string containing the class name of this datasource"""
		return type(self).__name__


	def get_description(self):
		"""Returns string containing a user-configured description for this datasource"""
		return self._description


	def register_database(self, db):
		"""Registers the database connection for this datasource.

		Args:
		  db (mixed): The database connection

		"""
		self._db = db


	def register_logger(self, logger):
		"""Registers the logger for this datasource.

		Args:
		  logger (mixed): Any object that implements the generic logging interface

		"""
		self._logger = logger


	def _finalize_report(self, successful=True):
		"""Completes the load metrics object and returns it.

		Args:
		  successful (bool, optional): Set whether the load can be considered a
		    success or failure.  Defaults to True.

		Returns:
		  LoadReport

		"""

		report = self._report

		report.time_elapsed = time.time() - self._time_started
		report.successful = successful

		return report


	def _get_config_param(self, key, required=False, default=None):
		"""Retrieves a specific configuration parameter

		Args:
		  key (string): The key for the param
		  required (bool): If set to True, function will throw error if param
		    value is empty
		  default (string): Specify a default value

		Returns:
		  string

		Raises:
		  SourceConfigurationError

		"""
		value = self._config.get(key, default)
		if required and not value:
			raise SourceConfigurationError(
				"Required config parameter '{}' is missing".format(key),
				self._config)
		else:
			return value


	def _increment_number_processed(self):
		"""Increments the total number of records which were processed (counts successes and failures). """
		self._report.number_processed += 1


	def _increment_number_successes(self):
		"""Increments the number of records which were successfully processed. """
		self._report.number_successes += 1


	def _log(self, message, *args, **kwargs):
		"""Facade for logging one-line messages.

		Args:
		  message (string): The message to be logged.
		  *args (list, optional): Any formatting components to be added to the message
		  **kwargs (dict, optional): Facade options.
		    `level` allows to specify the logging level for this message.

		Returns:
		  string: The log entry

		"""

		level = kwargs.get("level", "info").strip().lower()

		# Resolve the log level directly to a logger function
		logger_func = getattr(self._logger, level)
		return logger_func(message, *args,
		                   tag=self.get_identifier(),
		                   degree=1)
	

	def _logbanner(self, level, message, *args):
		"""Facade for logging banner messages.

		Args:
		  level (string): The log level to use.
		  message (string): The message to be logged.
		  *args (list, optional): Any formatting components to be added to the message

		Returns:
		  None

		"""
		self._logger.banner(level, message, *args,
		                    tag=self.get_identifier(),
		                    degree=1)
	

	def _save(self, data):
		"""Saves a single record to the data warehouse as a JSON document.

		Args:
		  data (mixed): An object that can be JSON-serialized.

		Raises:
		  DatabaseError

		"""
		self._db.warehouse(data, self.get_identifier(), data.created_on)


	def _start_timer(self):
		"""Begin keeping track of the processing time. """
		self._time_started = time.time()


	def _update_latest_record_date(self, new_date):
		"""Sets a new latest record date.

		Args:
		  new_date (string or datetime)

		"""

		# Convert date string to object
		if isinstance(new_date, str):
			new_date = _string_to_date(new_date)

		# Update the latest record metric with whatever the latest date is
		previous_date = self._report.latest_record
		self._report.latest_record = max(new_date, previous_date)


class LocalFileDataSource(BaseDataSource):
	"""This datasource scans a directory on the local filesystem for supported
	data files, moving them to an archive folder upon successful completion of
	all contained records.

	Args:
	  config (dict): The configuration parameter node for this datasource

	Attributes:
	  _queue (string): The path to the queue folder.
	  _archive (string): The path to the archive folder.

	"""
	def __init__(self, config):
		super().__init__(config)
		self._queue = self._get_config_param("queue_path", True)
		self._archive = self._get_config_param("archive_path", True)

	def execute(self):
		"""Executes the data load operation.

		Returns:
		  LoadReport: Contains the metrics for this load operation

		Raises:
		  DatabaseError
		  ExtractError

		"""

		class FakeData:
			pass

		# Do some fake stuff for now
		self._start_timer()
		self._logbanner("warn", "Nothing's actually happening here; we just block for a few seconds and return some fake metrics to show off how the thing works.")
		
		for i in range(2):
			fake_data = FakeData()
			fake_data.id = i
			fake_data.title = "lorem ipsum"
			fake_data.payload = "this is some fake data"
			fake_data.created_on = datetime.datetime.now()

			self._save(fake_data)

			self._log("Found fake record #{0:03d}", i)
			self._increment_number_processed()
			self._increment_number_successes()

		# Pretend something's take a while
		time.sleep(0.125)

		return self._finalize_report(True)


class LoadReport:
	"""A data struct used to contain load metrics for a load operation.

	Args:
	  datasource (BaseDataSource): The datasource which contains this report

	Attributes:
	  identifier (string): The identifier for the datasource it came from.
	  latest_record (datetime): The date of the latest record.
	  number_processed (int): The total number of records processed by this load operation.
	  number_successes (int): The number of records successfully processed by this load operation.
	  successful (bool): True if the operation was determined to be a success
	  time_elapsed (float): The number of seconds this load operation took.

	"""
	def __init__(self, datasource):
		self.identifier       = datasource.get_identifier()
		self.latest_record    = datetime.datetime.min
		self.number_processed = int()
		self.number_successes = int()
		self.successful       = False
		self.time_elapsed     = float(-1)

	def __str__(self):
		values = {
			"passfail":   "OK" if self.successful else "FAIL",
			"identifier": self.identifier,
			"successes":  self.number_successes,
			"processed":  self.number_processed,
			"elapsed":    self.time_elapsed
		}
		return _REPORT_FORMAT.format(**values)


def _string_to_date(datestring):
	"""Converts a string to a datetime object.

	@@TODO: Needs a lot more TLC

	Args:
	  datestring (string): A date in one of the following formats: @@@@@@@@@@@@@@@@@@@@@@@@@@@@

	Returns:
	  datetime

	"""

	# Detect the format
	import re
	import time

	datestring = datestring.strip()

	mapping = {}
	mapping[r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:[+-]\d{4})$"] = "%Y-%m-%dT%H:%M:%S" # ISO8601 (numeric timezone)
	mapping[r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\w+)$"]       = "%Y-%m-%dT%H:%M:%SEDT" # ISO8601 (named timezone)
	mapping[r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})$"]              = "%Y-%m-%dT%H:%M:%S" # ISO8601 (named timezone)

	for pattern, formatting in mapping.items():
		try:
			if re.match(pattern, datestring):
				time_struct = time.strptime(datestring, formatting)
				return datetime.datetime.fromtimestamp(time.mktime(time_struct), datetime.timezone.utc)
		except ValueError as e:
			print("Failed for ", str(e))
			continue

	raise CDLSError("Couldn't parse date '{}'".format(datestring))


