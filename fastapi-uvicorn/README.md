# IFCB Features Microservice - FastAPI + Uvicorn

REST API microservice for IFCB (Imaging FlowCytoBot) image segmentation and feature extraction using FastAPI and Uvicorn.

## API Endpoints

- `POST /process-bins` - Process multiple IFCB bins from ZIP archive
- `GET /health` - Health check endpoint

### Interactive Documentation

Once running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Installation & Setup

### Docker

1. **Build the image**:
```bash
cd fastapi-uvicorn
docker build -t ifcb-features-api .
```

2. **Run the container**:
```bash
docker run -p 8000:8000 ifcb-features-api
```

## Usage

### Process IFCB Bins from ZIP

Upload a ZIP file containing IFCB bin files. The ZIP should contain sets of `.adc`, `.roi`, and `.hdr` files:

```bash
curl -X POST "http://localhost:8000/process-bins" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@ifcb_bins.zip" \
  -o ifcb_results.zip
```

The response will be a ZIP file download containing features and blob masks.

**Example ZIP structure:**
```
ifcb_bins.zip
├── D20150314T203456_IFCB102.adc
├── D20150314T203456_IFCB102.roi
├── D20150314T203456_IFCB102.hdr
├── D20150315T104523_IFCB102.adc
├── D20150315T104523_IFCB102.roi
└── D20150315T104523_IFCB102.hdr
```

### Health Check

```bash
curl -X GET "http://localhost:8000/health"
```

## Response Format

The endpoint returns a ZIP file (`ifcb_results.zip`) containing:

```
ifcb_results.zip
├── features.json                              # All extracted features for all bins
├── D20150314T203456_IFCB102_blobs_v4.zip     # Blob masks for first bin
├── D20150315T104523_IFCB102_blobs_v4.zip     # Blob masks for second bin
└── ...
```

### features.json Structure

```json
{
  "bins_processed": 2,
  "total_rois": 150,
  "bins": [
    {
      "bin_id": "D20150314T203456_IFCB102",
      "roi_count": 75,
      "rois": [
        {
          "roi_number": 1,
          "features": {
            "Area": 1024,
            "Biovolume": 512.5,
            "Perimeter": 128.0,
            "EquivDiameter": 36.1,
            "MajorAxisLength": 45.2,
            "MinorAxisLength": 28.7,
            ...
          }
        },
        ...
      ]
    },
    ...
  ]
}
```

### Blob ZIP Structure

Each `*_blobs_v4.zip` contains PNG images of the segmented blob masks:

```
D20150314T203456_IFCB102_blobs_v4.zip
├── D20150314T203456_IFCB102_00001.png
├── D20150314T203456_IFCB102_00002.png
└── ...
```
