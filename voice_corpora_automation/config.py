"""Corpora automation configuration"""
import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

TIMESTAMP = datetime.now().isoformat()

CV_DATABASE_URL = os.getenv("CV_DATABASE_URL")
CV_EXPORT_FILENAME = os.getenv("CV_EXPORT_FILENAME", f"cv-export-{TIMESTAMP}.tsv")
CV_EXPORT_DIR = os.getenv("CV_EXPORT_DIR")
CV_S3_BUCKET = os.getenv("CV_S3_BUCKET")

CORPORA_EXPORT_DIR = os.getenv("CORPORA_EXPORT_DIR")
CORPORA_DATABASE_URL = os.getenv("CORPORA_DATABASE_URL")
CORPORA_DATABASE_TABLE = os.getenv("CORPORA_DATABASE_TABLE", default="cv_corpora")
CORPORA_S3_BUCKET = os.getenv("CORPORA_S3_BUCKET")
