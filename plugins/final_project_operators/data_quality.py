from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        for table in self.tables:
            self.log.info(f'Running data quality check for table {table}')
            records_count = redshift_hook.get_records(f'select count(*) from {table}')
            self.log.info(f'records count obtained is {records_count}')
            if len(records_count) < 1 or len(records_count[0]) < 1 or records_count[0][0] < 1:
                raise ValueError(f'Data qaulity check for table {table} failed - has 0 rows')
            else:
                self.log.info(f'Data quality check for table {table} passed - has more than 0 rows')