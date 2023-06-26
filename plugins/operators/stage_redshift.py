#!/usr/bin/python
# -*- coding: utf-8 -*-
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'
    unformatted_sql = \
        """
                            COPY {}
                            FROM 's3://{}/{}'
                            IAM_ROLE '{}'
                            FORMAT AS JSON 'auto';"""

    @apply_defaults
    def __init__(
        self,
        conn_id='redshift',
        s3_bucket='',
        path='',
        iam_role='',
        table='',
        *args,
        **kwargs
        ):

#                  aws_credintials='',

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

            # Map params here
            # Example:

        self.conn_id = conn_id

    #         self.aws_credintials = aws_credintials

        self.s3_bucket = s3_bucket
        self.table = table
        self.path = path
        self.iam_role =iam_role
    def execute(self, context):

#         self.log.info('StageToRedshiftOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        formatted_sql = self.unformatted_sql.format(self.table,
                self.s3_bucket, self.path, self.iam_role)
        redshift.run(formatted_sql)
