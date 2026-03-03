"""
pawn_queue.exceptions
~~~~~~~~~~~~~~~~~~~~~
All library-specific exception types.
"""

from __future__ import annotations


class PawnQueueError(Exception):
    """Base exception for all pawnqueue-sqs errors."""


class ConfigError(PawnQueueError):
    """Raised when configuration is missing or invalid."""


class TopicNotFoundError(PawnQueueError):
    """Raised when an operation targets a topic that has not been created."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(f"Topic {topic!r} does not exist. Call create_topic() first.")


class TopicAlreadyExistsError(PawnQueueError):
    """Raised when create_topic() is called with force=True on an existing topic."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        super().__init__(f"Topic {topic!r} already exists.")


class RegistrationError(PawnQueueError):
    """Raised when registering a producer or consumer fails."""


class LeaseConflictError(PawnQueueError):
    """Raised when the current instance failed to acquire a lease (another consumer won)."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id
        super().__init__(f"Could not acquire lease for message {message_id!r}. Another consumer claimed it.")


class LeaseExpiredError(PawnQueueError):
    """Raised when an operation is attempted on a message whose lease has expired."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id
        super().__init__(f"Lease for message {message_id!r} has expired.")


class PublishError(PawnQueueError):
    """Raised when publishing a message to S3 fails."""


class CompatibilityError(PawnQueueError):
    """Raised when the S3 backend does not support required operations."""
