import os
import pandas as pd
from sqlalchemy import create_engine, text
from django.conf import settings

TABLE_NAME = "dataset_current"

def get_engine():
    default = settings.DATABASES['default']
    if default['ENGINE'].endswith('sqlite3'):
        db_url = f"sqlite:///{default['NAME']}"
        eng = create_engine(
            db_url, pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 20}
        )
        with eng.connect() as con:
            con.exec_driver_sql("PRAGMA journal_mode=WAL;")
            con.exec_driver_sql("PRAGMA busy_timeout=20000;")
        return eng
    db_url = default.get('URL') or os.environ.get('DATABASE_URL')
    if not db_url:
        raise RuntimeError("Set DATABASE_URL for non-sqlite databases.")
    return create_engine(db_url, pool_pre_ping=True)

def read_any_table(file) -> pd.DataFrame:
    name = getattr(file, 'name', 'uploaded')
    ext = os.path.splitext(name)[1].lower()
    if ext in ['.xlsx', '.xls']:
        return pd.read_excel(file)
    else:
        try:
            return pd.read_csv(file, sep=None, engine='python')
        except Exception:
            file.seek(0)
            return pd.read_csv(file)

def load_dataframe_to_sql(df: pd.DataFrame):
    eng = get_engine()
    df.to_sql(TABLE_NAME, eng, if_exists='replace', index=False, method='multi', chunksize=1000)

def run_sql(query: str, params=None):
    eng = get_engine()
    with eng.connect() as conn:
        res = conn.execute(text(query), params or {})
        rows = res.fetchall()
        cols = res.keys()
    return [dict(zip(cols, r)) for r in rows]
