import argparse
import time
from datetime import datetime
from datetime import timedelta

from dotenv import load_dotenv
from alembic.config import Config
from alembic import command

from app import utils
from app.etl.pipeline import Pipeline
from app.etl.transformer import TransformCovidConfirmedCases
from app.etl.transformer import TransformCovidMortality
from app.etl.transformer import TransformCovidVaccinationByCategory
from app.etl.transformer import TransformDemographicData
from app.etl.transformer import TransformTotalNumberOfDeadsPerRegion
from app.models.models import CovidConfirmedCases, NSI_Code
from app.models.models import CovidMortality
from app.models.models import CovidVaccinationByCategory
from app.models.models import RegionDemographics
from app.models.models import TotalNumberOfDeadsPerRegions
from app.tools.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

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
        session.close()

def temp():
    nsi_codes_table = NSI_Code.__table__
    engine = utils.get_db_engine(echo=False)
    nsi_codes_table.drop(engine, checkfirst=True)
    nsi_codes_table.create(engine)
    print(NSI_Code.get_all())

def run():
    temp()

    
    logger.debug('Start ETL: %s', format(datetime.now()))
    start_time = time.time()

    if (args.migrate):
        run_migrations(downgrade_first = (args.migrate == 'downgrade_first'))

    # initialize db session
    pipelines = [
        Pipeline(
            CovidVaccinationByCategory,
            path="testdata/cov1.csv",
            # path="https://epistat.sciensano.be/Data/COVID19BE_VACC.csv",
            transformer=TransformCovidVaccinationByCategory(),
            # session=session,
        ),
        Pipeline(
            CovidMortality,
            path="testdata/mort1.csv",
            # path="https://epistat.sciensano.be/Data/COVID19BE_MORT.csv",
            transformer=TransformCovidMortality(),
            # session=session,
        ),
        Pipeline(
            CovidConfirmedCases,
            path="testdata/case1.csv",
            # path="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv",
            transformer=TransformCovidConfirmedCases(),
            # session=session,
        ),
        Pipeline(
            RegionDemographics,
            path="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2021.zip",
            transformer=TransformDemographicData(),
            # session=session,
        ),
        Pipeline(
            TotalNumberOfDeadsPerRegions,
            path="https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.zip",
            transformer=TransformTotalNumberOfDeadsPerRegion(),
            # session=session,
        ),
    ]

    # Als je voor develop purposes enkel bepaalde pipelines wilt importeren kan je indices opgeven
    # a[start:stop]  # items start through stop-1
    # a[start:]  # items start through the rest of the array
    # a[:stop]  # items from the beginning through stop-1
    # a[:]  # a copy of the whole array
    # proces enkel laatste  pipeline[-1:]
    for pipe in pipelines[-1:]:

        with utils.db_session() as session:
            start = time.time()
            start_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # print(f"Start of migration of model {pipe.data_class.__name__}: {start_string}")
            pipe.process(session)
            end = time.time()
            end_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            elapsed = end - start
            # print(f"End of migration of model {pipe.data_class.__name__}: {end_string}")
            # print(f"Time elapsed: {timedelta(seconds=elapsed)}")

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
    run()
