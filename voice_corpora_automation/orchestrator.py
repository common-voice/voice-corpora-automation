"""Corpora automation CLI"""
import os
import subprocess

from voice_corpora_automation import config
from voice_corpora_automation.exporter import FullDatasetExporter, DatasetDiffer


def main():
    """Run automation orchestration"""
    exporter = FullDatasetExporter()
    exporter.load_data()
    exporter.process_data()
    exporter.export()

    cv_filename = os.path.join(config.CV_EXPORT_DIR, config.CV_EXPORT_FILENAME)
    cmd = f"create-corpora -d {config.CORPORA_EXPORT_DIR} -f {cv_filename}"
    subprocess.run(cmd, capture_output=True, check=True)

    differ = DatasetDiffer(exporter.dataframe)
    differ.prepare()
    differ.write()


if __name__ == "__main__":
    main()
