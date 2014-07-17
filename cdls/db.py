import datetime
import json
import os
import sqlite3
import uuid

import cdls.config

from cdls.errors import DatabaseError

_DBPATH = cdls.config.DB_SQLITE_PATH

_DDL_CREATE_LOADSTATS = """
CREATE TABLE `CDLS_LOAD_STATS`
(
	 `IDENTIFIER`         TEXT(64)
	,`ATTEMPTED_ON`       TEXT(24)
	,`SUCCESSFUL`         INT
	,`TOTAL_RECORDS`      INT
	,`LATEST_RECORD_DATE` TEXT(24)
	,`REMARKS`            TEXT(200)
)
"""

_DDL_DROP_LOADSTATS = "DROP TABLE `CDLS_LOAD_STATS`"

_DDL_CREATE_WAREHOUSE = """
CREATE TABLE `WAREHOUSE`
(
	 `GUID`               TEXT(40)
	,`SOURCE_IDENTIFIER`  TEXT(64)
	,`RECORD_DATE`        TEXT(20)
	,`JSON`               TEXT(16000)
)
"""
_DDL_DROP_WAREHOUSE = "DROP TABLE `WAREHOUSE`"

_conn = None


def install():
	"""Initializes the database. """
	with _connect() as connection:

		# Initialize the Loadstats table
		try:
			connection.execute(_DDL_DROP_LOADSTATS)
		except sqlite3.OperationalError:
			pass

		connection.execute(_DDL_CREATE_LOADSTATS)

		# Initialize the Warehouse table
		try:
			connection.execute(_DDL_DROP_WAREHOUSE)
		except sqlite3.OperationalError:
			pass

		_execute_query(connection, _DDL_CREATE_WAREHOUSE)


def warehouse(data, source, record_date):
	"""Lazy way of decomposing a data structure by simply saving it as a JSON
	document with a bunch of metadata.

	Args:
	  data (mixed): Any object that can be serialized to JSON.
	  source (string): The identifier for the datasource where the data came
	    from.
	  record_date (datetime): The object .

	Raises:
	  DatabaseError

	"""
	query = """
INSERT INTO warehouse
	( guid,  source_identifier,  record_date,  json)
VALUES
	(:guid, :source_identifier, :record_date, :json)
"""
	params = {}

	# Package the data first
	data_type = type(data).__name__
	packaged_data = {
		"$class": data_type,
		"$contents": data
	}

	# Serialize to JSON
	json_string = None
	try:
		json_string = json.dumps(packaged_data, default=_tojson, sort_keys=True, indent=4)
	except TypeError:
		raise DatabaseError("JSON-serialization failed on data")

	# Perform insert
	params["json"] = json_string.strip()
	params["guid"] = str(uuid.uuid1()).upper()
	params["source_identifier"] = source.strip().upper()
	params["record_date"] = _date_to_string(record_date)

	with _connect() as connection:
		_execute_query(connection, query, params)


def _connect():
	"""Returns a connection to the database. """
	global _conn
	if not _conn:
		_conn = sqlite3.connect(_DBPATH)
	return _conn


def _date_to_string(date):
	"""Formats a datetime into a string. """
	# timezones... le sigh
	return date.strftime("%Y-%m-%d %H:%M:%S.%f")


def _execute_query(connection, query, params=None):
	"""Standard operation for executing a SQL query against the database.

	Args:
	  connection (db): The connection to the database.
	  query (string): The SQL to be executed.
	  params (dict, optional): Any SQL parameters to be bound.

	Raises:
	  DatabaseError

	"""
	query = query.strip()
	try:
		if params:
			connection.execute(query, params)
		else:
			connection.execute(query)
	except sqlite3.OperationalError as e:
		raise DatabaseError("sqlite3: {}".format(e), query, params) from e


def _tojson(o):
	"""Rudimentary parse handler for JSON serialization.

	Args:
	  o (mixed): The object to be serialized.

	"""
	if isinstance(o, datetime.datetime):
		return _date_to_string(o)
	else:
		return o.__dict__

