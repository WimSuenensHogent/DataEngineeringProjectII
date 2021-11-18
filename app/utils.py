import os
import contextlib

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.tools.logger import get_logger

logger = get_logger(__name__)


def get_db_type():
    # return "mssql"
    TYPE = os.environ.get("DATABASE_TYPE")
    # print("TYPE")
    # print(TYPE)

    if (not TYPE) or (TYPE != "mssql"):
        TYPE = "sqlite"
        URL = os.environ.get("DATABASE_URL")
        if URL:
            if "mssql" in URL.lower():
                TYPE = "mssql"

    return TYPE


def get_db_url():
    # return "mssql://localhost:localhost@localhost/covid_data?driver=ODBC Driver 17 for SQL Server"
    URL = os.environ.get("DATABASE_URL")

    # TYPE = os.environ.get('DATABASE_TYPE')
    TYPE = get_db_type()
    SERVER = os.environ.get("DATABASE_SERVER")
    DATABASE = os.environ.get("DATABASE_DATABASE")
    UID = os.environ.get("DATABASE_UID")
    PWD = os.environ.get("DATABASE_PWD")

    if TYPE:
        if TYPE == "mssql" and SERVER and DATABASE and UID and PWD:
            URL = "{TYPE}+pyodbc:///?odbc_connect=DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD}".format(
                TYPE=TYPE, SERVER=SERVER, DATABASE=DATABASE, UID=UID, PWD=PWD
            )

    if not URL:
        # URL = 'sqlite:///database.sqlite'
        URL = "sqlite://"
    # print(URL)
    return URL
    # return os.environ.get('DATABASE_URL') or 'sqlite:///database.sqlite'


def get_db_engine(echo=False):
    url = get_db_url()
    engine = create_engine(url, echo=echo)
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


# Print iterations progress
def printProgressBar(
    iteration,
    total,
    prefix="",
    suffix="",
    decimals=1,
    length=100,
    fill="#",
    printEnd="\r",
):
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
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()
