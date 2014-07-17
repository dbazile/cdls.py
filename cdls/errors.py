"""All custom errors used by the CDLS.
"""

import pprint

class CDLSError(Exception):
	"""The base class for all exceptions raised by the CDLS.

	Args:
	  message (string): The exception message

	"""
	def __init__(self, message):
		super().__init__(message)


	def details(self):
		"""Stub method that subclasses should extend to provide more detailed error
		information.

		Returns:
		  Subclasses should return a string

		"""
		pass


class DatabaseError(CDLSError):
	"""Represents a failure to connect to or query the local database. """
	def __init__(self, original_exception, query=None, params=None):
		super().__init__(str(original_exception))
		self._query = query

		assert isinstance(params, dict) or params is None
		self._params = params

	def details(self):
		output = ""
		if self._query:
			output += "SQL Query:\n" + self._query

			if self._params:
				output += "Parameters:\n"
				for k, v in self._params.items():
					output += "{0:16s} : {1}\n".format(k, v)

			return output


class ExtractError(CDLSError):
	"""Represents a failure to set up or perform an extraction. """
	pass


class SourceConfigurationError(CDLSError):
	"""Represents a failure to read or retrieve source configuration data.

	Args:
	  message (string): The exception message
	  node (Element): The XML source configuration node

	Attributes:
	  _node (dict, optional): The specific config data node containing the error
	    the source configuration file where an error was detected.

	"""
	def __init__(self, message, node=None):
		super().__init__(message)
		self._node = node


	def details(self):
		"""Returns the data node from the source config file.

		Returns:
		  string

		"""
		if self._node:
			return "Config Node:\n" + pprint.pformat(self._node)


class UnregisteredSourceError(CDLSError):
	"""Represents an attempt to perform actions on a given source identifier
	which has not yet been registered in the CDLS.

	Args:
	  identifier (string): The source's given identifier

	"""
	def __init__(self, identifier):
		super().__init__("Source {} is not registered".format(identifier))
