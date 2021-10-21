import argparse
import logging
import time
from datetime import datetime

import yaml
from alembic.config import Config
from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker

from alembic import command
from app import utils
from app.etl.pipeline import Pipeline
from app.etl.transformer import TransformCovidConfirmedCases
from app.etl.transformer import TransformCovidMortality
from app.etl.transformer import TransformCovidVaccinationByCategory
from app.models.models import CovidConfirmedCases
from app.models.models import CovidMortality
from app.models.models import CovidVaccinationByCategory
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
        #todo: remove voor productiebuild
        command.downgrade(alembic_cfg, "base") # doet een downgrade vd db en gooit alles weg
        command.upgrade(alembic_cfg, "head")

        data_pipeline_vaccinations = Pipeline(
            CovidVaccinationByCategory,
            csv_path="testdata/cov1.csv",
            # csv_path="https://epistat.sciensano.be/Data/COVID19BE_VACC.csv",
            transformer=TransformCovidVaccinationByCategory(),
            session=session,
        )
        pipelines.append(data_pipeline_vaccinations)

        print("update vacc")

        data_pipeline_mortality = Pipeline(
            CovidMortality,
            csv_path="testdata/mort1.csv",
            # csv_path="https://epistat.sciensano.be/Data/COVID19BE_MORT.csv",
            transformer=TransformCovidMortality(),
            session=session,
        )
        pipelines.append(data_pipeline_mortality)

        data_pipeline_confirmed_cases = Pipeline(
            CovidConfirmedCases,
            csv_path="testdata/case1.csv",
            # csv_path="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv",
            transformer=TransformCovidConfirmedCases(),
            session=session,
        )
        pipelines.append(data_pipeline_confirmed_cases)

        print(datetime.now())
        print("--- %s minutes ---" % ((time.time() - start_time) / 60))
        logger.info("Finsished ETL: {}".format(datetime.now()))

        print("update mort")
        # except Exception as exception:
        #     logger.error(exception)

    for pipe in pipelines:
        pipe.process()


if __name__ == "__main__":
    run()
