#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    unformatted_sql = \
        """
                             insert into {}
                             {}
                             """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        sql_query='',
        conn_id='',
        table='',
        *args,
        **kwargs
        ):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql_query = sql_query
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=conn_id)
        formatted_sql = self.sql_query.format(self.table,
                self.sql_query)
        redshift.run(formatted_sql)
