"""Custom exception types for the Permutive API wrapper."""


class PermutiveAPIError(Exception):
    """Base exception class for all PermutiveAPI errors.

    This exception is raised for errors that are specific to the PermutiveAPI
    library.
    """

    pass


class PermutiveAuthenticationError(PermutiveAPIError):
    """Raised when authentication fails (HTTP 401 or 403).

    This typically indicates an invalid API key or insufficient permissions for
    the requested resource.
    """

    pass


class PermutiveBadRequestError(PermutiveAPIError):
    """Raised for client-side errors (HTTP 400).

    This indicates a problem with the request itself, such as a malformed
    payload or invalid parameters. The message from the API is included.
    """

    def __init__(self, message: str, *args):
        """Initialise the PermutiveBadRequestError.

        Parameters
        ----------
        message : str
            The error message.
        """
        self.message = message
        super().__init__(message, *args)


class PermutiveResourceNotFoundError(PermutiveAPIError):
    """Raised when a requested resource is not found (HTTP 404)."""

    pass


class PermutiveRateLimitError(PermutiveAPIError):
    """Raised when the API rate limit is exceeded (HTTP 429)."""

    pass


class PermutiveServerError(PermutiveAPIError):
    """Raised for server-side errors (HTTP 5xx).

    This indicates a problem on Permutive's end. These errors may be transient,
    and retries are handled by the RequestHelper before this is raised.
    """

    pass
