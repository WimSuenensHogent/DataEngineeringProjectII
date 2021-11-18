"""fact_covid_mortality_by_category

Revision ID: 11a7ec4e32c9
Revises: 218262db97ee
Create Date: 2021-11-15 00:00:56.914715

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "11a7ec4e32c9"
down_revision = "218262db97ee"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "fact_covid_mortality_by_category",
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("region", sa.String(255), nullable=False),
        sa.Column("agegroup", sa.String(32), nullable=False),
        sa.Column("sex", sa.String(5), nullable=False),
        sa.Column("deaths", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("date", "region", "agegroup", "sex"),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("fact_covid_mortality_by_category")
    # ### end Alembic commands ###
