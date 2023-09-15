import configparser
import psycopg2
import logging
from sql_queries import (
    copy_table_queries,
    insert_table_queries,
    staging_events_check_queries,
    staging_songs_check_queries,
    count_table_length_queries,
)


# ETL PIPELINE FUNCTIONS
# Fill and check staging tables
def load_staging_tables(cur, conn):
    """Function to run the SQL queries inserting data into the DWH staging tables. Includes quality check for missing values, written to log file."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    # Data quality check: Check for missing values in staging_events
    for query in staging_events_check_queries:
        cur.execute(query)
        results = cur.fetchall()

        for result in results:
            column_name, null_count = result
            if null_count > 0:
                logging.warning(
                    f"Data quality check failed: Staging table 'staging_events' contains {null_count} missing values in column '{column_name}'."
                )

    # Data quality check: Check for missing values in staging_songs
    for query in staging_songs_check_queries:
        cur.execute(query)
        results = cur.fetchall()

        for result in results:
            column_name, null_count = result
            if null_count > 0:
                logging.warning(
                    f"Data quality check failed: Staging table 'staging_songs' contains {null_count} missing values in column '{column_name}'."
                )


# Fill final tables
def insert_tables(cur, conn):
    """Function to run the SQL queries inserting data into the DWH tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    # Check for successful filling and count rows 
    for query in count_table_length_queries:
        cur.execute(query)
        result = cur.fetchone()
        print(f" Number of rows is {result}")


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # Configure logging
    logging.basicConfig(
        filename="additional_material/etl.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    try:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"ETL process failed: {str(e)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
