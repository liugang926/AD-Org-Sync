"""Self-service password reset bounded context.

SSPR must stay outside the sync runtime orchestration. Future implementation
should expose service-level entry points here, then let Web/CLI adapters call
those services rather than reaching into target providers directly.
"""

