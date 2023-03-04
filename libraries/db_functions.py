from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def connect_to_db(db_url: str) -> Engine:
    mydb = create_engine(db_url)
    return mydb

def execute_sql_from_file(file: str, db: Engine):
    with db.connect() as conn:
        with open(file) as f:
            query = text(f.read())
            conn.execute(query)

def execute_sql_query(query: str, db: Engine):
    with db.connect() as conn:
        conn.execute(text(query))
        
def disconnect_db(db: Engine):
    db.dispose()