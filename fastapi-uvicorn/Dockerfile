FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Clone and install ifcb-features from main branch
RUN git clone https://github.com/WHOIGit/ifcb-features.git /tmp/ifcb-features \
    && cd /tmp/ifcb-features \
    && git checkout main \
    && pip install -e . \
    && cd / \
    && rm -rf /tmp/ifcb-features/.git

# Copy application code
COPY ./app /app/app

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
