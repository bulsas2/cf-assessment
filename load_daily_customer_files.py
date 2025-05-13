import json
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from abc import ABC, abstractmethod
from loguru import logger
import csv

class FileToDbLoaderInterface(ABC):
    def __init__(self, db_config, file_path, table_name):
        self.db_config = db_config
        self.file_path = file_path
        self.table_name = table_name

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def load_file(self):
        raise NotImplementedError

    @abstractmethod
    def persist_to_table(self):
        raise NotImplementedError


class CsvToPostgresLoader(FileToDbLoaderInterface):
    def __init__(self, db_config, file_path, table_name):
        super().__init__(db_config, file_path, table_name)

    def run(self):
        logger.info(f"Loading file {self.file_path} into table {self.table_name}")
        self.persist_to_table()

    def load_file(self):
        return pd.read_csv(self.file_path)

    def persist_to_table(self):
        df = self.load_file()

        staging_table_name = f"{self.table_name}_staging"
        old_table_name = f"{self.table_name}_old"

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                conn.autocommit = True

                logger.debug(f"Creating staging table {staging_table_name}")
                cur.execute(
                    sql.SQL(
                        """
                    DROP TABLE IF EXISTS {staging};
                    CREATE TABLE {staging} (
                        customer_number INTEGER,
                        customer_rating INTEGER,
                        customer_rating_limit INTEGER,
                        customer_status VARCHAR(25)
                    );
                """
                    ).format(staging=sql.Identifier(staging_table_name))
                )

                logger.debug(f"Loading data into staging table {staging_table_name}")
                with open(self.file_path, "r") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            # Validate customer_number is a valid integer (z1111753583, 11119b88202)
                            customer_number = int(row["customer_number"])
                            customer_rating = int(row["customer_rating"])
                            customer_rating_limit = int(row["customer_rating_limit"])
                            customer_status = row["customer_status"].strip()

                            cur.execute(
                                sql.SQL("""
                                    INSERT INTO {staging} 
                                    (customer_number, customer_rating, customer_rating_limit, customer_status)
                                    VALUES (%s, %s, %s, %s)
                                """).format(staging=sql.Identifier(staging_table_name)),
                                (customer_number, customer_rating, customer_rating_limit, customer_status)
                            )
                        except (ValueError, KeyError) as e:
                            logger.warning(f"Detected invalid row: {e} ({row})")
                            # There can be more than one invalid column. For this assessment, just take the first one (which is customer_number).

                            invalid_column = None
                            # int types
                            for col in ["customer_number", "customer_rating", "customer_rating_limit"]:
                                try:
                                    _ = int(row[col])
                                except (ValueError, KeyError):
                                    invalid_column = col
                                    break

                            if invalid_column is None and "customer_status" not in row:
                                invalid_column = "customer_status"

                            # Insert invalid row into file_invalid_rows table
                            json_data = json.dumps(row)
                            cur.execute("""
                                INSERT INTO file_invalid_rows (file, invalid_column, table_name, payload)
                                VALUES (%s, %s, %s, %s)
                                """,
                                (
                                    self.file_path,
                                    invalid_column,
                                    self.table_name,
                                    json.dumps(row),
                                )
                            )


                logger.debug("Analyze staging table before replacing.")
                cur.execute(
                    sql.SQL("ANALYZE {staging};").format(
                        staging=sql.Identifier(staging_table_name)
                    )
                )

                logger.debug(f"Renaming {self.table_name} to {old_table_name}")
                cur.execute(
                    sql.SQL(
                        """
                    DROP TABLE IF EXISTS {old};
                    ALTER TABLE IF EXISTS {master} RENAME TO {old};
                """
                    ).format(
                        old=sql.Identifier(old_table_name),
                        master=sql.Identifier(self.table_name),
                    )
                )

                logger.debug(
                    f"Renaming staging table {staging_table_name} to {self.table_name}"
                )
                cur.execute(
                    sql.SQL(
                        """
                    ALTER TABLE {staging} RENAME TO {master};
                """
                    ).format(
                        staging=sql.Identifier(staging_table_name),
                        master=sql.Identifier(self.table_name),
                    )
                )

                logger.info(f"Finished loading data into {self.table_name}")

def clear_file_invalid_rows_table(db_config):
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DROP TABLE IF EXISTS file_invalid_rows;
                CREATE TABLE file_invalid_rows (
                    id SERIAL PRIMARY KEY,
                    file VARCHAR(255),
                    invalid_column VARCHAR(255),
                    table_name VARCHAR(255),
                    payload JSONB
                );
                """
            )
            conn.commit()
    
    logger.debug("Cleared file_invalid_rows table before run")

if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "CustomerRatingData",
        "user": "postgres",
        "password": "postgres",
    }

    # Stage environment: Clear the invalid rows table before running again.
    clear_file_invalid_rows_table(db_config=db_config)

    # Load file for Agency A
    file_loader_agency_a = CsvToPostgresLoader(
        db_config=db_config,
        file_path="./data/Customer Rating Agency A Inc.csv",
        table_name="customer_rating_agency_a",
    )
    file_loader_agency_a.run()

    # Load file for Agency B
    file_loader_agency_b = CsvToPostgresLoader(
        db_config=db_config,
        file_path="./data/Customer Rating Agency B Inc.csv",
        table_name="customer_rating_agency_b",
    )
    file_loader_agency_b.run()

    # Load file for Agency C
    file_loader_agency_c = CsvToPostgresLoader(
        db_config=db_config,
        file_path="./data/Customer Rating Agency C Inc.csv",
        table_name="customer_rating_agency_c",
    )
    file_loader_agency_c.run()

    # Load file for Agency D
    file_loader_agency_d = CsvToPostgresLoader(
        db_config=db_config,
        file_path="./data/Customer Rating Agency D Inc.csv",
        table_name="customer_rating_agency_d",
    )
    file_loader_agency_d.run()