import io
import pandas as pd
from sqlalchemy import create_engine
from django.conf import settings
from describir_dataset_agnostico import describir_dataset

def _build_sqlalchemy_url():
    default = settings.DATABASES['default']
    if default['ENGINE'].endswith('sqlite3'):
        return f"sqlite:///{default['NAME']}"
    url = default.get('URL')
    if url:
        return url
    raise RuntimeError("Define DATABASE_URL/URL para motores no-sqlite.")

def describe_current_table():
    engine = create_engine(_build_sqlalchemy_url())
    df = pd.read_sql("SELECT * FROM dataset_current", engine)
    buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
    _, resumen = describir_dataset(buf, export_pdf=None)
    return resumen
