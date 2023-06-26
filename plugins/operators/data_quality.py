from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    unformated_sql= 'select * from {} limit 20'
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id = '',
                 table = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = conn_id
        self.table = table
    def execute(self, context):
        formatted_sql = self.unformatted_sql.format(table)
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        records = redshift_hook.get_records(formatted_sql)
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no       results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        