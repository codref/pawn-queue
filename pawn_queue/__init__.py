"""
pawnqueue-sqs
~~~~~~~~~~
S3-compatible pub/sub queue library.

Turn any S3-compatible object store into a lightweight message queue
without requiring an AWS account.  Works with Hetzner Object Storage,
MinIO, Cloudflare R2, and AWS S3.

Quickstart::

    import asyncio
    from pawn_queue import PawnQueue

    async def main():
        async with await PawnQueue.from_config("config.yaml") as pawnqueue:
            await pawnqueue.create_topic("orders")

            producer = await pawnqueue.register_producer("order-service")
            await producer.publish("orders", {"order_id": 42})

            consumer = await pawnqueue.register_consumer("billing", topics=["orders"])
            messages = await consumer.poll()
            for msg in messages:
                print(msg.payload)
                await msg.ack()

    asyncio.run(main())
"""

from .config import PawnQueueConfig
from .consumer import Consumer
from .core import PawnQueue, PawnQueueBuilder
from .exceptions import (
    CompatibilityError,
    ConfigError,
    LeaseConflictError,
    LeaseExpiredError,
    PawnQueueError,
    PublishError,
    RegistrationError,
    TopicAlreadyExistsError,
    TopicNotFoundError,
)
from .message import Message
from .producer import Producer

__all__ = [
    "PawnQueue",
    "PawnQueueBuilder",
    "PawnQueueConfig",
    "Producer",
    "Consumer",
    "Message",
    # Exceptions
    "PawnQueueError",
    "ConfigError",
    "TopicNotFoundError",
    "TopicAlreadyExistsError",
    "RegistrationError",
    "LeaseConflictError",
    "LeaseExpiredError",
    "PublishError",
    "CompatibilityError",
]

__version__ = "0.1.0"

