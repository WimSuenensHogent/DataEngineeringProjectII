"""initial

Revision ID: 0c51e3c78861
Revises:
Create Date: 2021-10-19 18:27:52.575984

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0c51e3c78861"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "covid_vaccinations_by_category",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("region", sa.String(), nullable=True),
        sa.Column("agegroup", sa.String(), nullable=True),
        sa.Column("sex", sa.String(), nullable=True),
        sa.Column("brand", sa.String(), nullable=True),
        sa.Column("dose", sa.String(), nullable=True),
        sa.Column("count", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "covid_confirmed_cases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("province", sa.String(), nullable=True),
        sa.Column("region", sa.String(), nullable=True),
        sa.Column("agegroup", sa.String(), nullable=True),
        sa.Column("sex", sa.String(), nullable=True),
        sa.Column("cases", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "covid_mortality",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=True),
        sa.Column("region", sa.String(), nullable=True),
        sa.Column("agegroup", sa.String(), nullable=True),
        sa.Column("sex", sa.String(), nullable=True),
        sa.Column("deaths", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("covid_vaccinations_by_category")
    op.drop_table("covid_mortality")
    op.drop_table("covid_confirmed_cases")
