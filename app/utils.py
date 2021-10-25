import os
import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.tools.logger import get_logger

logger = get_logger(__name__)

def get_db_url():
    return os.environ.get('DATABASE_URL') or 'sqlite:///database.sqlite'

def get_db_engine(echo=False):
    engine = create_engine(get_db_url(), echo=echo)
    return engine

@contextlib.contextmanager
# def db_session(settings):
def db_session(echo=False):
    # engine = sa.engine_from_config(settings, "sqlalchemy.")
    engine = get_db_engine(echo=echo)
    session_maker = sessionmaker(engine)
    with session_maker() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
