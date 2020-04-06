"""Corpora dataset exporter"""
import hashlib
import os

import pandas
import sqlalchemy

from voice_corpora_automation import config
from voice_corpora_automation import queries


class FullDatasetExporter:
    """Exporter of the complete dataset from CV (excluding clips)"""

    def __init__(self):
        self.engine = sqlalchemy.create_engine(config.CV_DATABASE_URL)
        self.query = queries.FULL_DATASET_QUERY
        self.dataframe = None

    def load_data(self):
        """Read SQL data from CV database to a pandas DataFrame"""
        self.dataframe = pandas.read_sql(self.query, self.engine)
        return self.dataframe

    def process_data(self):
        """Preprocess `client_id`, `path`, `sentence`"""
        hasher = lambda x: hashlib.sha512(x).hexdigest()
        renamer = lambda x: f"common_voice_{x['locale']}_{x['id']}.mp3"
        self.dataframe["client_id"] = self.dataframe["client_id"].apply(hasher)
        self.dataframe["path"] = self.dataframe.apply(renamer)
        self.dataframe["sentence"] = self.dataframe["sentence"].str.replace("\r", " ")
        self.dataframe.rename(columns={"client_id": "hashed_client_id"})

    def export(self):
        """Export DataFrame to TSV format"""
        path = os.path.join(config.CV_EXPORT_DIR, config.CV_EXPORT_FILENAME)
        self.dataframe.to_csv(path, sep="\t", index=False)


class DatasetDiffer:
    """Differ of the complete dataset from CV"""

    def __init__(self):
        self.engine = sqlalchemy.create_engine(config.CORPORA_DATABASE_URL)
        self.corpora_path = config.CORPORA_EXPORT_DIR
        self.dataframe = None
        self.diff = None
        self.last_timestamp = None

    def load(self):
        """Load the output of the corpora creator to a TSV"""
        partitions = ["dev", "invalid", "other", "test", "train", "valid"]
        for locale in os.listdir(self.corpora_path):
            self.dataframe = pandas.DataFrame()
            for part in partitions:
                path = os.path.join(self.corpora_path, locale, f"{part}.tsv")
                partial_df = pandas.read_csv(path, sep="\t")
                partial_df["locale"] = locale
                partial_df["partition"] = part
                partial_df["timestamp"] = config.TIMESTAMP
            self.dataframe = pandas.concat(self.dataframe, partial_df)

    def get_last_timestamp(self):
        """Get the latest `timestamp` from corpora database"""
        metadata = sqlalchemy.MetaData(bind=self.engine, reflect=True)
        session = sqlalchemy.orm.sessionmaker(bind=self.engine)()
        return session.query(sqlalchemy.func.max(metadata.tables["timestamp"])).scalar()

    def prepare(self):
        """Filter only rows with `timestamp` greater than the latest timestamp available"""
        last_timestamp = self.get_last_timestamp()
        self.diff = self.dataframe[self.dataframe["timestamp"] > last_timestamp]

    def write(self):
        """Write `diff` dataframe to the corpora database"""
        self.diff.to_sql(
            config.CORPORA_DATABASE_TABLE, self.engine, if_exists="append", index=False,
        )
