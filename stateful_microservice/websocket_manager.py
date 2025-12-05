"""WebSocket connection manager for real-time job updates."""

import base64
from pathlib import Path
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

    def _wrap_message(self, msg_type: str, data: dict) -> dict:
        """Wrap data in standardized message format."""
        return {
            "type": msg_type,
            "data": data
        }

    def send_update(self, job_id: str, data: dict, msg_type: str = "progress"):
        """
        Send update to WebSocket from any thread (including worker threads).

        Args:
            job_id: Job ID
            data: JSON-serializable dict to send
            msg_type: Message type (default: "progress")
        """
        if job_id not in self._connections:
            return

        if self._event_loop is None:
            logger.warning(f"Event loop not available for WebSocket update for job {job_id}")
            return

        ws = self._connections[job_id]

        # Wrap the data in standardized message format
        message = self._wrap_message(msg_type, data)

        try:
            # Schedule the coroutine to run on the main event loop from this thread
            asyncio.run_coroutine_threadsafe(
                ws.send_json(message),
                self._event_loop
            )
            logger.debug(f"Sent WebSocket {msg_type} message for job {job_id}")
        except Exception as e:
            logger.error(f"Failed to send WebSocket update for job {job_id}: {e}")

    def send_artifact(
        self, 
        job_id: str, 
        file_path: Path, 
        content_type: Optional[str] = None
    ):
        """
        Send a file in a websocket message.

        Args:
            job_id: Job ID for routing to correct websocket.
            file_path: Path to file to send.
            content_type: MIME type (auto-detected if not specified)
        """
        if not file_path.exists():
            logger.error(f"Artifact file not found {file_path}")
            return

        artifact_name = file_path.name

        # Guess content type if not provided
        if content_type is None:
            import mimetypes
            content_type, _ = mimetypes.guess_type(file_path)
            content_type = content_type or "application/octet-stream"

        try:
            # Read entire file
            with open(file_path, 'rb') as f:
                file_data = f.read()

            size_bytes = len(file_data)

            # Encode to base64
            encoded_data = base64.b64encode(file_data).decode('utf-8')

            # Send as artifact message
            self.send_update(job_id, {
                "name": artifact_name,
                "data": encoded_data,
                "size_bytes": size_bytes,
                "content_type": content_type
            }, msg_type="artifact")

            logger.info(f"Sent artifact {artifact_name} ({size_bytes} bytes) to job {job_id}")

        except Exception as e:
            logger.error(f"Failed to send artifact {artifact_name}: {e}", exc_info=True)


# Global singleton instance
websocket_manager = WebSocketManager()
