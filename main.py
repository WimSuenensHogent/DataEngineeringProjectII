from app.etl.transformer import Transformer
from app.models.models import Base, CovidVacinationByCategory
from app.tools.logger import get_logger
from app.tools.database import Database
from app.etl.pipeline import Pipeline

logger = get_logger(__name__)


def run():
    try:
        database = Database(Base)
        database.run_migrations()

        data_pipeline = Pipeline(
            database,
            CovidVacinationByCategory,
            csv_path="C:/Users/ilya/Desktop/testdata/cov1.csv",
            #csv_path="https://epistat.sciensano.be/Data/COVID19BE_VACC.csv",
            transformer=Transformer(
                column_renamer={
                    "DATE": "date",
                    "REGION": "region",
                    "AGEGROUP": "agegroup",
                    "SEX": "sex",
                    "BRAND": "brand",
                    "DOSE": "dose",
                    "COUNT": "count",
                }
            ),
        )

    except Exception as exception:
        logger.error(exception)

    data_pipeline.process()


if __name__ == "__main__":
    run()
