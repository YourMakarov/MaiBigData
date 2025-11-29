FROM ghcr.io/lakehq/flink:1.17.2-python3.10

WORKDIR /opt/flink/job
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY flink_star_job.py dimension_enrichment.py enriched_record.py ./
ENTRYPOINT ["python3", "flink_star_job.py"]
