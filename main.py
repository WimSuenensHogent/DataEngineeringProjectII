import argparse
import ssl

from dotenv import load_dotenv
from alembic.config import Config
from alembic import command

from app import utils
from app import _app
from app.tools.logger import get_logger

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()
logger = get_logger(__name__)

def run():
    return _app.run()

    # initialize db session
    # pipelines = [
    #     Pipeline(
    #         DailyUpdateOnVaccinationNumberPerNISCode,
    #         path="https://www.laatjevaccineren.be/vaccination-info/get/vaccinaties.csv",
    #         #path="testdata/test.csv",
    #         transformer=TransformTotalNumberOfVaccinationsPerNICCode(),
    #         # session=session,
    #         isLast=True
    #     ),
    #     Pipeline(
    #         WekelijkseVaccinatiesPerNISCode,
    #         path="https://epistat.sciensano.be/data/COVID19BE_VACC_MUNI_CUM.csv",
    #         transformer=TransformWekelijkseVaccinatiesPerNISCode(),
    #         # session=session,
    #     )
    # ]

def run_migrations(downgrade_first=False):
    # run db migrations
    with utils.db_session() as session:
        alembic_cfg = Config("alembic.ini")
        # pylint: disable=unsupported-assignment-operation
        alembic_cfg.attributes["connection"] = session.bind

        if (downgrade_first):
            command.downgrade(
                alembic_cfg, "base"
            )  # doet een downgrade vd db en gooit alles weg

        command.upgrade(alembic_cfg, "head")
        print("upgraded..")
        session.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the covid data ETL job.")
    parser.add_argument(
        "-m",
        "--migrate",
        help="Do you want to run database migrations at first?",
        choices=["upgrade", "downgrade_first"]
    )
    # Parsing YAML file
    args = parser.parse_args()
    if (args.migrate):
        run_migrations(downgrade_first=(args.migrate == 'downgrade_first'))
    run()
