"""WebSocket connection manager for real-time job updates."""

import asyncio
import logging
from typing import Dict, Optional

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages WebSocket connections and sends updates from worker threads."""

    def __init__(self):
        self._connections: Dict[str, WebSocket] = {}
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop):
        """Store reference to the main event loop (called at startup)."""
        self._event_loop = loop
        logger.info("WebSocket manager initialized with event loop")

    def register(self, job_id: str, websocket: WebSocket):
        """Register a WebSocket connection for a job."""
        self._connections[job_id] = websocket
        logger.debug(f"Registered WebSocket for job {job_id}")

    def unregister(self, job_id: str):
        """Unregister a WebSocket connection."""
        if job_id in self._connections:
            del self._connections[job_id]
            logger.debug(f"Unregistered WebSocket for job {job_id}")

    def send_update(self, job_id: str, data: dict):
        """
        Send update to WebSocket from any thread (including worker threads).

        Args:
            job_id: Job ID
            data: JSON-serializable dict to send
        """
        if job_id not in self._connections:
            return

        if self._event_loop is None:
            logger.warning(f"Event loop not available for WebSocket update for job {job_id}")
            return

        ws = self._connections[job_id]

        try:
            # Schedule the coroutine to run on the main event loop from this thread
            asyncio.run_coroutine_threadsafe(
                ws.send_json(data),
                self._event_loop
            )
            logger.debug(f"Sent WebSocket update for job {job_id}")
        except Exception as e:
            logger.error(f"Failed to send WebSocket update for job {job_id}: {e}")


# Global singleton instance
websocket_manager = WebSocketManager()
