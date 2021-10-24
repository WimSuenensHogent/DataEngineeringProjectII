import argparse
import logging
import time
from datetime import datetime
from datetime import timedelta

import yaml
from alembic.config import Config

from alembic import command
from app import utils
from app.etl.pipeline import Pipeline
from app.etl.transformer import TransformCovidConfirmedCases
from app.etl.transformer import TransformCovidMortality
from app.etl.transformer import TransformCovidVaccinationByCategory
from app.etl.transformer import TransformDemographicData
from app.etl.transformer import TransformTotalNumberOfDeadsPerRegion
from app.models.models import CovidConfirmedCases
from app.models.models import CovidMortality
from app.models.models import CovidVaccinationByCategory
from app.models.models import RegionDemographics
from app.models.models import TotalNumberOfDeadsPerRegions
from app.tools.logger import get_logger

logger = get_logger(__name__)
pipelines = []


def run():
    # Parsing YAML file
    parser = argparse.ArgumentParser(description="Run the covid data ETL job.")
    parser.add_argument("config", help="A configuration file in YAML format.")
    args = parser.parse_args()
    config = yaml.safe_load(open(args.config))

    # configure logging
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    logger.info("Start ETL: {}".format(datetime.now()))
    start_time = time.time()

    # initialize db session
    with utils.db_session(config) as session:
        # run db migrations
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.attributes["connection"] = session.bind
        # todo: remove voor productiebuild
        command.downgrade(
            alembic_cfg, "base"
        )  # doet een downgrade vd db en gooit alles weg
        command.upgrade(alembic_cfg, "head")

        pipelines = [
            Pipeline(
                CovidVaccinationByCategory,
                path="testdata/cov1.csv",
                # path="https://epistat.sciensano.be/Data/COVID19BE_VACC.csv",
                transformer=TransformCovidVaccinationByCategory(),
                session=session,
            ),
            Pipeline(
                CovidMortality,
                path="testdata/mort1.csv",
                # path="https://epistat.sciensano.be/Data/COVID19BE_MORT.csv",
                transformer=TransformCovidMortality(),
                session=session,
            ),
            Pipeline(
                CovidConfirmedCases,
                path="testdata/case1.csv",
                # path="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv",
                transformer=TransformCovidConfirmedCases(),
                session=session,
            ),
            Pipeline(
                RegionDemographics,
                path="https://statbel.fgov.be/sites/default/files/files/opendata/bevolking%20naar%20woonplaats%2C%20nationaliteit%20burgelijke%20staat%20%2C%20leeftijd%20en%20geslacht/TF_SOC_POP_STRUCT_2021.zip",
                transformer=TransformDemographicData(),
                session=session,
            ),
            Pipeline(
                TotalNumberOfDeadsPerRegions,
                path="https://statbel.fgov.be/sites/default/files/files/opendata/deathday/DEMO_DEATH_OPEN.zip",
                transformer=TransformTotalNumberOfDeadsPerRegion(),
                session=session,
            ),
        ]

        logger.info("Finsished ETL: {}".format(datetime.now()))

        print("update mort")
        # except Exception as exception:
        #     logger.error(exception)

    # Als je voor develop purposes enkel bepaalde pipelines wilt importeren kan je indices opgeven
    # a[start:stop]  # items start through stop-1
    # a[start:]  # items start through the rest of the array
    # a[:stop]  # items from the beginning through stop-1
    # a[:]  # a copy of the whole array
    # proces enkel laatste  pipeline[-1:]

    for pipe in pipelines[-1:]:
        start = time.time()
        start_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Start of migration of model {pipe.data_class.__name__}: {start_string}")
        pipe.process()
        end = time.time()
        end_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed = end - start
        print(f"End of migration of model {pipe.data_class.__name__}: {end_string}")
        print(f"Time elapsed: {timedelta(seconds=elapsed)}")


if __name__ == "__main__":
    run()
