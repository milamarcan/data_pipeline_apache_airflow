from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_query="",
                 append_mode=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query,
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator is starting')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_mode == True:
            redshift.run(self.sql_query)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            redshift.run(self.sql_query)
        self.log.info('LoadDimensionOperator is finished')
