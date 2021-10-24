import os
import contextlib

from sqlalchemy import create_engine
from sqlalchemy import orm

from app.tools.logger import get_logger

logger = get_logger(__name__)

def get_db_url():
    return os.environ.get('DATABASE_URL') or 'sqlite:///database.sqlite'

def get_db_engine():
    engine = create_engine(get_db_url())
    return engine

@contextlib.contextmanager
# def db_session(settings):
def db_session():
    # engine = sa.engine_from_config(settings, "sqlalchemy.")
    engine = get_db_engine()
    session = orm.sessionmaker(bind=engine)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
