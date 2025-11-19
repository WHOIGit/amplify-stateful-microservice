"""IFCB Features Extraction Microservice."""

from stateful_microservice import create_app
from processor import FeaturesProcessor

# Create FastAPI app with features processor
app = create_app(FeaturesProcessor())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
