"""Corpora dataset exporter"""
import csv
import hashlib
import logging
import os

import pandas
import sqlalchemy

from voice_corpora_automation import config
from voice_corpora_automation import queries

LOGGER = logging.getLogger(__name__)


class FullDatasetExporter:
    """Exporter of the complete dataset from CV (excluding clips)"""

    def __init__(self):
        self.engine = sqlalchemy.create_engine(config.CV_DATABASE_URL, echo=True)
        self.query = queries.FULL_DATASET_QUERY
        self.dataframe = None

    def load_data(self):
        """Read SQL data from CV database to a pandas DataFrame"""
        LOGGER.info("Reading data from CV database;")
        self.dataframe = pandas.read_sql(self.query, self.engine)
        return self.dataframe

    def process_data(self):
        """Preprocess `client_id`, `path`, `sentence`"""
        LOGGER.info("Processing fields")
        hasher = lambda x: hashlib.sha512(x.encode("utf-8")).hexdigest()
        renamer = lambda x: f"common_voice_{x.locale}_{x.id}.mp3"
        self.dataframe["client_id"] = self.dataframe["client_id"].apply(hasher)
        self.dataframe["path"] = self.dataframe.apply(renamer, axis=1)
        self.dataframe["sentence"] = self.dataframe["sentence"].str.replace("\r", " ")
        self.dataframe.rename(columns={"client_id": "hashed_client_id"})

    def export(self):
        """Export DataFrame to TSV format"""
        LOGGER.info("Store dataset as TSV")
        path = os.path.join(config.CV_EXPORT_DIR, config.CV_EXPORT_FILENAME)
        self.dataframe.to_csv(path, sep="\t", index=False, quoting=csv.QUOTE_NONE)


class DatasetDiffer:
    """Differ of the complete dataset from CV"""

    def __init__(self, full_dataset):
        self.engine = sqlalchemy.create_engine(config.CORPORA_DATABASE_URL)
        self.corpora_path = config.CORPORA_EXPORT_DIR
        self.full_dataset = full_dataset
        self.dataframe = None
        self.diff = None

    def load(self):
        """Load the output of the corpora creator to a TSV"""
        partitions = ["dev", "invalidated", "other", "test", "train", "validated"]
        self.dataframe = pandas.DataFrame()

        LOGGER.info("Loading corpora")
        for locale in os.listdir(self.corpora_path):
            for part in partitions:
                LOGGER.info("Locale: %s", locale)
                LOGGER.info("Part: %s", part)
                path = os.path.join(self.corpora_path, locale, f"{part}.tsv")
                partial_df = pandas.read_csv(path, sep="\t")
                partial_df["locale"] = locale
                partial_df["partition"] = part
                partial_df["timestamp"] = config.TIMESTAMP
                self.dataframe = pandas.concat([self.dataframe, partial_df])

    def prepare(self):
        """Filter out entries already exist in a previous version"""
        LOGGER.info("Prepare diff for corpora")
        try:
            LOGGER.info("Calculating diff for the new version")
            current_corpora = pandas.read_sql(
                config.CORPORA_DATABASE_TABLE, self.engine
            )
            self.diff = self.dataframe[~self.dataframe.path.isin(current_corpora.path)]
        except:
            LOGGER.info("Something went wrong, falling back to using the whole")
            self.diff = self.dataframe

    def write(self):
        """Write `diff` dataframe to the corpora database"""
        LOGGER.info("Write diff to corpora DB")
        self.diff.to_sql(
            config.CORPORA_DATABASE_TABLE, self.engine, if_exists="append", index=False,
        )
