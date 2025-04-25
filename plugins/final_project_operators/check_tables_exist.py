from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class CheckTablesExistOperator(BaseOperator):
    """
    Checks whether specified tables exist in the given Redshift/Postgres database.
    """

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id,
        tables,
        schema='public',
        *args, **kwargs
    ):
        super(CheckTablesExistOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables
        self.schema = schema

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info(f"Checking existence of tables in schema '{self.schema}': {self.tables}")
        query = f"""
            SELECT DISTINCT tablename
            FROM pg_table_def
            WHERE schemaname = '{self.schema}'
        """

        records = redshift.get_records(query)
        existing_tables = {row[0] for row in records}
        missing_tables = set(self.tables) - existing_tables

        if missing_tables:
            raise ValueError(f"The following tables are missing: {missing_tables}")

        self.log.info(f"All tables found: {self.tables}")