import os
import sys
from pathlib import Path

# ingestion.py lee estas variables al importarse; las pruebas no usan S3.
os.environ.setdefault("S3_BUCKET", "test-bucket")
os.environ.setdefault("AWS_REGION", "us-east-2")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
