

# CODE FROM: https://github.com/aio-libs/aiohttp/issues/850
class FailedRequest(Exception):
    """
    A wrapper of all possible exception during a HTTP request
    """

    def __init__(self, raised=None, message=None, status_code=None, url=None):
        """
        
        Args:
            raised: Exception class name
            message: Exception message
            status_code: Status code of response
            url: request url
        """
        self.raised = raised
        self.message = message
        self.status_code = status_code
        self.url = url

        super().__init__(self.get_error_msg())

    def get_error_msg(self):
        return f"code:{self.status_code} url={self.url} message={self.message} raised={self.raised}"

class NotRetryResponseStatusCode(Exception):
    pass