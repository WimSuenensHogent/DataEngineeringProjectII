from app.etl.transformer import Transformer
from app.models.models import Base, CovidVaccinationByCategory, CovidMortality, CovidConfirmedCases
from app.tools.logger import get_logger
from app.tools.database import Database
from app.etl.pipeline import Pipeline

logger = get_logger(__name__)
pipelines = []

def run():
    try:
        database = Database(Base)

        data_pipeline_vaccinations = Pipeline(
            database,
            CovidVaccinationByCategory,
            csv_path="testdata/cov1.csv",
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
            )
        )
        pipelines.append(data_pipeline_vaccinations)

        print("update vacc")

        data_pipeline_mortality = Pipeline(
            database,
            CovidMortality,
            csv_path="testdata/mort1.csv",
            # csv_path="https://epistat.sciensano.be/Data/COVID19BE_MORT.csv",
            transformer=Transformer(
                column_renamer={
                    "DATE": "date",
                    "REGION": "region",
                    "AGEGROUP": "agegroup",
                    "SEX": "sex",
                    "DEATHS": "deaths",
                }
            ),
        )
        pipelines.append(data_pipeline_mortality)

        data_pipeline_confirmed_cases = Pipeline(
            database,
            CovidConfirmedCases,
            csv_path="testdata/case1.csv",
            # csv_path="https://epistat.sciensano.be/Data/COVID19BE_CASES_AGESEX.csv",
            transformer=Transformer(
                column_renamer={
                    "DATE": "date",
                    "PROVINCE": "province",
                    "REGION": "region",
                    "AGEGROUP": "agegroup",
                    "SEX": "sex",
                    "CASES": "cases",
                }
            ),
        )
        pipelines.append(data_pipeline_confirmed_cases)


        print("update mort")
    except Exception as exception:
        logger.error(exception)

    for pipe in pipelines: pipe.process()


if __name__ == "__main__":
    run()
