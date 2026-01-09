# Use Python 3.10 slim (or whichever you prefer)
FROM python:3.10-slim

# Set working directory
WORKDIR /opt/dagster/app

# Copy requirements first for caching
COPY requirements.txt .

# Install pip dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Ensure dagster and dagit are installed
RUN pip install dagster dagit dagster-dbt

# Copy the rest of your app
COPY . .

# Expose port for Dagit
EXPOSE 3000

# Default command (can be overridden by docker-compose)
CMD ["dagit", "-h", "0.0.0.0", "-p", "3000", "-f", "/opt/dagster/app/dagster_pipeline.py"]
