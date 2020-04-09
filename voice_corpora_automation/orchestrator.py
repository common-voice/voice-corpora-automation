"""Corpora automation CLI"""
import os
import sys

from corporacreator.tool import run as corporacreator_run

from voice_corpora_automation import config
from voice_corpora_automation.exporter import FullDatasetExporter, DatasetDiffer


def main():
    """Run automation orchestration"""
    exporter = FullDatasetExporter()
    exporter.load_data()
    exporter.process_data()
    exporter.export()

    cv_filename = os.path.join(config.CV_EXPORT_DIR, config.CV_EXPORT_FILENAME)
    sys.argv = ["corpora-creator", "-f", cv_filename, "-d", config.CORPORA_EXPORT_DIR]
    corporacreator_run()

    differ = DatasetDiffer(exporter.dataframe)
    differ.load()
    differ.prepare()
    differ.write()
    differ.sync_s3()


if __name__ == "__main__":
    main()
