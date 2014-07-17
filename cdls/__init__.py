"""
Cloudy Data Load Subsystem

Attributes:
  _datasources (dict): Once a datasource is registered, it will be stored in
    this hash.
  _db (mixed): Any object or module that implements the database interface.
  _logger (mixed): Any object or module that implements the standard logging
    interface (i.e., info, warning, error, exception).

"""

import datetime
import importlib
import json
import os

from . import db
from . import config
from . import logging
from . import errors
from . import datasources
from cdls.errors import (DatabaseError, SourceConfigurationError, UnregisteredSourceError, CDLSError)

_datasources = {}
_db = None
_logger = None

def initialize():
	"""Intended to fully initialize the CDLS so that it can begin accepting
	load requests.

	Raises:
	  DatabaseError
	  SourceConfigurationError
	  CDLSError

	"""

	# Initialize logging to the logging module
	register_logger(logging)

	# Attempt to connect to database
	register_database(db)

	# Read the source configuration from disk
	source_configurations = _get_source_configurations(config.PATH_SOURCECONFIG)

	# Register datasources into the CDLS
	_register_all_datasources(source_configurations)


def list_registered_sources():
	"""Gets a list of all registered sources.

	Returns:
	  tuple: (identifier, type)

	"""

	def source_to_tuple(ds):
		return (ds.get_identifier(), ds.get_type(), ds.get_description())

	return [source_to_tuple(ds) for (k, ds) in sorted(_datasources.items())]


def perform_all_loads(halt_on_error=False):
	"""Executes a load operation on every registered source in the CDLS.

	Args:
	  halt_on_error (bool): If True, will fail fast instead of attempting to
	    perform a load on the next source.

	Returns:
	  list of LoadReport

	Raises:
	  DatabaseError
	  ExtractError

	"""
	_logger.info("Loading all sources")
	reports = []
	for datasource in _datasources.values():
		try:
			reports.append(datasource.execute())
		except CDLSError as e:
			_logger.exception(e)
			if halt_on_error:
				raise e

	# List results for everything
	_logger.info("Loads complete")
	N = len(reports)
	for n, report in enumerate(reports):
		_logger.info(str(report))

	return tuple(reports)


def perform_load(identifier):
	"""Executes a load on a single datasource.

	Args:
	  identifier (string): The identifier for the datasource to be executed.

	Returns:
	  LoadReport

	Raises:
	  DatabaseError
	  ExtractError

	"""
	identifier = identifier.strip()
	try:
		datasource = _datasources[identifier]
	except KeyError:
		raise UnregisteredSourceError(identifier)

	try:
		_logger.info("Attempting load for '{}'", identifier)
		load_report = datasource.execute()
		_logger.info(str(load_report))
		return load_report
	except CDLSError as e:
		_logger.exception(e)
		raise e


def register_database(db):
	"""Registers a database connection to be used by all registered components.

	Args:
	  db (mixed): Anything that implements the database interface.

	Raises:
	  DatabaseError

	"""
	global _db
	try:
		_db = db
	except DatabaseError as e:
		_logger.exception(e)
		raise e


def register_datasource(datasource):
	"""Registers a single datasource into the CDLS.

	Args:
	  datasource (BaseDataSource): The instantiated datasource to be registered.

	Raises:
	  SourceConfigurationError

	"""
	try:
		datasource.register_logger(_logger)
		datasource.register_database(_db)
		_datasources[datasource.get_identifier()] = datasource
	except CDLSError as e:
		_logger.exception(e)
		raise e


def register_logger(logger):
	"""Initialize logging mechanism.

	Args:
	  logger (mixed): Anything that implements the generic logging interface

	"""
	global _logger
	try:
		# Make sure this thing can actually log stuff
		assert hasattr(logger, "info")
		assert hasattr(logger, "error")
		assert hasattr(logger, "warn")
		assert hasattr(logger, "exception")

		_logger = logger

	except AssertionError:
		print("Fatal Error ** Failed to initialize logger")
		exit(1)


def _get_qualified_class_ref(config_node):
	"""Returns a class reference for a given source configuration node.

	Args:
	  config_node (dict): The source configuration node.

	Returns:
	  class

	Raises:
	  SourceConfigurationError

	"""
	try:
		(module_name, class_name) = config_node["@QualifiedClassName"].rsplit(".", 1)

		# Get module
		module_ref = importlib.import_module(module_name)

		# Return a reference to the class
		try:
			return getattr(module_ref, class_name)
		except AttributeError:
			raise SourceConfigurationError(str(e))
	except KeyError:
		raise SourceConfigurationError("@QualifiedClassName property is missing", config_node)
	except ImportError as e:
		raise SourceConfigurationError(str(e), config_node) from e


def _get_source_configurations(source_config_path):
	"""Retrieves all registered sources from the source configuration file.

	Args:
	  source_config_path (string): The path to the source configuration file.

	Returns:
	  tuple: A tuple of source configuration nodes (dictionaries)

	Raises:
	  SourceConfigurationError

	"""
	if os.path.exists(source_config_path):
		try:
			# Parse the configurations from JSON text file
			with open(source_config_path) as fp:
				options = json.load(fp)

			try:
				return tuple(options["registered"])
			except KeyError:
				raise SourceConfigurationError("Source config is missing the 'registered' node")
		except ValueError as e:
			raise SourceConfigurationError("JSON parse failed on source file '{0}': {1}".format(source_config_path, str(e)))
	else:
		raise SourceConfigurationError("File '{}' does not exist".format(source_config_path))


def _register_all_datasources(source_configurations):
	"""Registers all datasources from the configuration collection.

	Args:
	  source_configurations (tuple of dict)

	Raises:
	  SourceConfigurationError

	"""

	try:
		for config_node in source_configurations:
			_DataSourceClassReference = _get_qualified_class_ref(config_node)

			assert issubclass(_DataSourceClassReference, datasources.BaseDataSource)

			datasource = _DataSourceClassReference(config_node)
			register_datasource(datasource)

	except SourceConfigurationError as e:
		_logger.exception(e)
		raise e


