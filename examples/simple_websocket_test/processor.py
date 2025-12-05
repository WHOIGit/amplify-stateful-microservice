"""Simple test processor that generates and streams a blue PNG."""

import tempfile
import time
from pathlib import Path

from PIL import Image
from pydantic import BaseModel

from stateful_microservice import BaseProcessor
from stateful_microservice.processor import JobInput


class SimpleResult(BaseModel):
    """Simple result."""
    message: str


class SimpleTestProcessor(BaseProcessor):
    """Generates a solid blue PNG and streams it via WebSocket."""

    @property
    def name(self) -> str:
        return "simple-test"

    @property
    def version(self) -> str:
        return "1.0.0"

    result_model = SimpleResult

    def process_input(self, job_input: JobInput) -> SimpleResult:
        """Generate 10 blue PNGs and stream them with 2-second delays."""

        temp_dir = Path(tempfile.mkdtemp())
        total_images = 10

        for i in range(total_images):
            # Report progress
            percent = (i / total_images) * 100
            self.report_progress("processing", percent=percent, message=f"Generating image {i+1}/{total_images}")

            # Create a 200x200 blue image
            img = Image.new('RGB', (200, 200), color=(0, 0, 255))

            # Save with unique name
            image_path = temp_dir / f"blue_square_{i+1}.png"
            img.save(image_path, format="PNG")

            # Stream it via WebSocket
            self.send_artifact(job_input.job_id, image_path)

            # Wait 2 seconds before next image (except after the last one)
            if i < 9:
                time.sleep(2)

        # Report completion
        self.report_progress("processing", percent=100.0, message=f"Completed all {total_images} images")

        return SimpleResult(message="Sent 10 blue_square images")
