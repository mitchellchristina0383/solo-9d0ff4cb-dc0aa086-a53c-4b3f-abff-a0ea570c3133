# Zero third-party Python dependencies: the whole app uses the standard
# library, so no pip install layer is needed.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/tracing.db

WORKDIR /workspace

COPY app/ /workspace/app/
COPY tests/ /workspace/tests/
COPY run.py /workspace/run.py

# No PYTHONPATH needed: run.py inserts the project root itself, which also
# works under isolated interpreters. /data is a mounted volume so windows
# survive container restarts.
RUN mkdir -p /data && adduser --disabled-password --gecos "" appuser \
    && chown -R appuser:appuser /data /workspace
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/api/health', timeout=2).status==200 else 1)"

CMD ["python", "-I", "run.py"]
