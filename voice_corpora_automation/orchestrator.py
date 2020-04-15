"""Corpora automation CLI"""
import os
import sys

from corporacreator.tool import run as corporacreator_run

from voice_corpora_automation import config
from voice_corpora_automation.exporter import DatasetUploader, DatasetExporter


def main():
    """Run automation orchestration"""
    exporter = DatasetExporter()
    exporter.load_data()
    exporter.process_data()
    exporter.prepare_diff()
    exporter.export_diff()

    cv_filename = os.path.join(config.CV_EXPORT_DIR, config.CV_EXPORT_FILENAME)
    sys.argv = ["corpora-creator", "-f", cv_filename, "-d", config.CORPORA_EXPORT_DIR]
    corporacreator_run()

    uploader = DatasetUploader(exporter.dataframe)
    uploader.load()
    uploader.upload()
    uploader.sync_s3()
