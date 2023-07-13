from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('LoadDimensionOperator is starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #formatted_sql = LoadDimensionOperator.load_sql.format(self.table, self.sql_query)
        redshift.run(self.sql_query)
        # redshift.run(formatted_sql)
        self.log.info('LoadDimensionOperator is finished')
