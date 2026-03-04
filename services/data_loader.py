import duckdb
from config import *

con = duckdb.connect(database=":memory:")


def register_tables():

    con.execute(f"""
        CREATE VIEW papers AS
        SELECT * FROM read_csv_auto('{PAPERS}', delim='\t');
    """)

    con.execute(f"""
        CREATE VIEW authors AS
        SELECT * FROM read_csv_auto('{AUTHORS}', delim='\t');
    """)

    con.execute(f"""
        CREATE VIEW paper_authors AS
        SELECT * FROM read_csv_auto('{PAPER_AUTHORS}', delim='\t');
    """)

    con.execute(f"""
        CREATE VIEW paper_references AS
        SELECT * FROM read_csv_auto('{REFERENCES}', delim='\t');
    """)
    con.execute(f"""
                CREATE VIEW paper_fields AS
                SELECT * FROM read_csv_auto(
                '{PAPER_FIELDS}',
                delim='\t'
                );
                """)
    con.execute(f"""
                CREATE VIEW fields AS
                SELECT * FROM read_csv_auto(
                '{FIELDS}',
                delim='\t'
                );
                """)
    con.execute(f"""
                CREATE VIEW affiliations AS
                SELECT * FROM read_csv_auto(
                '{AFFILIATIONS}',
                delim='\t'
                );
                """)
    con.execute(f"""
                CREATE VIEW patents AS
                SELECT * FROM read_csv_auto(
                '{PATENTS}',
                delim='\t'
                );
                """)