"""Entry point for simple WebSocket test service."""

from stateful_microservice import create_app
from processor import SimpleTestProcessor

app = create_app(SimpleTestProcessor())
