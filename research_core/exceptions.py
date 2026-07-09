class ResearchCoreError(ValueError):
    """Base error for invalid research core domain objects."""


class RequiredFieldError(ResearchCoreError):
    """Raised when a required domain field is missing or empty."""
