import contextlib

import sqlalchemy as sa
from sqlalchemy import orm


@contextlib.contextmanager
def db_session(settings):
    engine = sa.engine_from_config(settings, "sqlalchemy.")
    session = orm.sessionmaker(bind=engine)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
