FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies and uv
RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy pyproject.toml and uv.lock for dependency installation
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Clone and install ifcb-features from main branch
RUN git clone https://github.com/WHOIGit/ifcb-features.git /tmp/ifcb-features \
    && cd /tmp/ifcb-features \
    && git checkout main \
    && uv pip install -e . \
    && cd / \
    && rm -rf /tmp/ifcb-features/.git

# Copy application code
COPY ./app /app/app

# Expose port
EXPOSE 8000

# Run the application using uv
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
