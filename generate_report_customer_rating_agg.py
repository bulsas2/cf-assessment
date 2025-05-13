from datetime import datetime
import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from abc import ABC, abstractmethod
from loguru import logger
import csv


class ReportInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def generate_report(self):
        raise NotImplementedError

    @abstractmethod
    def save_report(self):
        raise NotImplementedError


class CustomerRatingAggregatedReport(ReportInterface):
    def __init__(self, db_config, report_output_path):
        super().__init__()
        self.db_config = db_config
        self.report_path = report_output_path

    def run(self):
        logger.info("Generating customer rating report")
        report_df = self.generate_report()
        self.save_report(report_df)

    def generate_report(self) -> pd.DataFrame:
        report_df: pd.DataFrame
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        """
                            WITH base AS (
                                SELECT 
                                    'agency_a' AS customer,
                                    customer_number,
                                    customer_rating,
                                    customer_rating_limit,
                                    customer_status
                                FROM customer_rating_agency_a
                                UNION ALL
                                SELECT 
                                    'agency_b' AS customer,
                                    customer_number,
                                    customer_rating,
                                    customer_rating_limit,
                                    customer_status
                                FROM customer_rating_agency_b
                                UNION ALL
                                SELECT 
                                    'agency_c' AS customer,
                                    customer_number,
                                    customer_rating,
                                    customer_rating_limit,
                                    customer_status
                                FROM customer_rating_agency_c
                                UNION ALL
                                SELECT 
                                    'agency_d' AS customer,
                                    customer_number,
                                    customer_rating,
                                    customer_rating_limit,
                                    customer_status
                                FROM customer_rating_agency_d
                            ),

                            base_agg AS (
                                SELECT
                                    customer,
                                    COUNT(customer_number) AS count_customer_number,
                                    SUM(customer_rating) AS sum_customer_rating,
                                    COUNT(CASE WHEN customer_status = 'high-value' THEN 1 ELSE NULL END) AS high_value_count
                                FROM base
                                GROUP BY customer
                            ),

                            invalid_agg AS (
                                SELECT
                                    CASE
                                        WHEN table_name = 'customer_rating_agency_a' THEN 'agency_a'
                                        WHEN table_name = 'customer_rating_agency_b' THEN 'agency_b'
                                        WHEN table_name = 'customer_rating_agency_c' THEN 'agency_c'
                                        WHEN table_name = 'customer_rating_agency_d' THEN 'agency_d'
                                    END AS customer,
                                    COUNT(*) AS invalid_count
                                FROM file_invalid_rows
                                WHERE invalid_column = 'customer_number'
                                GROUP BY table_name
                            )
                            SELECT
                                b.customer,
                                b.count_customer_number,
                                b.sum_customer_rating,
                                b.high_value_count,
                                COALESCE(i.invalid_count, 0) AS invalid_customer_number_count,
                                ROUND(
                                    COALESCE(i.invalid_count::decimal, 0) / NULLIF(b.count_customer_number + COALESCE(i.invalid_count, 0), 0) * 100,
                                    2
                                ) AS invalid_customer_number_percent
                            FROM base_agg b
                            LEFT JOIN invalid_agg i ON b.customer = i.customer
                            ORDER BY b.customer;
                        """
                    )
                )
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                report_df = pd.DataFrame(rows, columns=columns)

        return report_df

    def save_report(self, report_df: pd.DataFrame):
        filename = f"Customer_Rating_Aggregate_Report_{datetime.today().strftime('%Y%m%d')}.csv"

        report_file = os.path.join(self.report_path, filename)
        os.makedirs(self.report_path, exist_ok=True)

        report_df.to_csv(report_file, index=False)
        logger.info(f"Report saved to {report_file}")


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "CustomerRatingData",
        "user": "postgres",
        "password": "postgres",
    }

    report = CustomerRatingAggregatedReport(
        db_config=db_config, report_output_path="output"
    )
    report.run()
