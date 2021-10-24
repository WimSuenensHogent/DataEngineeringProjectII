import os
import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.tools.logger import get_logger

logger = get_logger(__name__)

def get_db_url():
    return os.environ.get('DATABASE_URL') or 'sqlite:///database.sqlite'

def get_db_engine():
    engine = create_engine(get_db_url(), echo=True)
    return engine

@contextlib.contextmanager
# def db_session(settings):
def db_session():
    # engine = sa.engine_from_config(settings, "sqlalchemy.")
    engine = get_db_engine()
    session_maker = sessionmaker(engine)
    sess = session_maker()
    with session_maker() as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '#', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()
