"""initial

Revision ID: 0c51e3c78861
Revises:
Create Date: 2021-10-19 18:27:52.575984

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0c51e3c78861"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "covid_confirmed_cases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("province", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("agegroup", sa.String(), nullable=False),
        sa.Column("sex", sa.String(), nullable=False),
        sa.Column("cases", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "covid_mortality",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("agegroup", sa.String(), nullable=False),
        sa.Column("sex", sa.String(), nullable=False),
        sa.Column("deaths", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "covid_vaccinations_by_category",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("region", sa.String(), nullable=False),
        sa.Column("agegroup", sa.String(), nullable=False),
        sa.Column("sex", sa.String(), nullable=False),
        sa.Column("brand", sa.String(), nullable=False),
        sa.Column("dose", sa.String(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "region_demographics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("municipality_niscode", sa.String(), nullable=False),
        sa.Column("municipality_name", sa.String(), nullable=False),
        sa.Column("district_niscode", sa.String(), nullable=False),
        sa.Column("district_name", sa.String(), nullable=False),
        sa.Column("province_niscode", sa.String(), nullable=False),
        sa.Column("province_name", sa.String(), nullable=False),
        sa.Column("region_niscode", sa.String(), nullable=False),
        sa.Column("region_name", sa.String(), nullable=False),
        sa.Column("sex", sa.String(), nullable=False),
        sa.Column("nationality_code", sa.String(), nullable=False),
        sa.Column("nationality_name", sa.String(), nullable=False),
        sa.Column("marital_status_code", sa.String(), nullable=False),
        sa.Column("marital_status_name", sa.String(), nullable=False),
        sa.Column("age", sa.Integer(), nullable=False),
        sa.Column("population", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "total_number_of_deads_per_region",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("district_niscode", sa.String(), nullable=False),
        sa.Column("province_niscode", sa.String(), nullable=False),
        sa.Column("region_niscode", sa.String(), nullable=False),
        sa.Column("sex", sa.String(), nullable=False),
        sa.Column("agegroup", sa.String(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("weak", sa.String(), nullable=False),
        sa.Column("number_of_deaths", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("total_number_of_deads_per_region")
    op.drop_table("region_demographics")
    op.drop_table("covid_vaccinations_by_category")
    op.drop_table("covid_mortality")
    op.drop_table("covid_confirmed_cases")
