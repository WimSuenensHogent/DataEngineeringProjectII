"""fact_vaccinations_by_nis_code_and_week

Revision ID: b89feedf3563
Revises: 7e4a579fbad8
Create Date: 2021-11-15 22:49:38.846236

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b89feedf3563'
down_revision = '7e4a579fbad8'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('fact_vaccinations_by_nis_code_and_week',
    sa.Column('date', sa.Date(), nullable=False),
    sa.Column('year', sa.Integer(), nullable=False),
    sa.Column('week', sa.Integer(), nullable=False),
    sa.Column('nis_code', sa.String(length=5), nullable=False),
    sa.Column('agegroup', sa.String(length=5), nullable=False),
    sa.Column('dose', sa.String(length=5), nullable=False),
    sa.Column('cumul_of_week', sa.Integer(), nullable=False),
    sa.CheckConstraint('length(nis_code)==5'),
    sa.PrimaryKeyConstraint('date', 'year', 'week', 'nis_code', 'agegroup', 'dose')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('fact_vaccinations_by_nis_code_and_week')
    # ### end Alembic commands ###
