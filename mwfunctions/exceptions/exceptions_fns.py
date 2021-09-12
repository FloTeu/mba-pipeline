import logging
from contextlib import AbstractContextManager, suppress


class log_suppress(AbstractContextManager):
    """ Log but suppress exception """
    def __init__(self, msg, *exceptions, logger=None):
        self._msg = msg
        self._exceptions = exceptions

        self.logger = logger if logger else logging.getLogger(__name__)


    def __enter__(self):
        pass

    def __exit__(self, exctype, excinst, exctb):
        # Unlike isinstance and issubclass, CPython exception handling
        # currently only looks at the concrete type hierarchy (ignoring
        # the instance and subclass checking hooks). While Guido considers
        # that a bug rather than a feature, it's a fairly hard one to fix
        # due to various internal implementation details. suppress provides
        # the simpler issubclass based semantics, rather than trying to
        # exactly reproduce the limitations of the CPython interpreter.
        #
        # See http://bugs.python.org/issue12029 for more details
        ret = exctype is not None and issubclass(exctype, self._exceptions)
        if ret:
            self.logger.exception(self._msg)
        # if ret == false -> reraise exception
        return ret


class log_if_except(AbstractContextManager):
    """ Log an exception with logging """

    def __init__(self, msg, *discard, logger=None):
        self._msg = msg
        self.logger = logger if logger else logging.getLogger(__name__)

    def __enter__(self):
        return None

    def __exit__(self, exctype, excinst, exctb):
        ret = exctype is not None
        if ret:
            self.logger.exception(self._msg)
        # if ret == false -> reraise exception
        return not ret