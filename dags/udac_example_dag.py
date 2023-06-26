from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'abdelrahman',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id='redshift',
    s3_bucket=Variable.get('s3_bucket'),
    path = 'log_data',
    iam_role = Variable.get('iam_role'),
    table='staging_events'
    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id='redshift',
    s3_bucket=Variable.get('s3_bucket'),
    path = 'song_data',
    iam_role = Variable.get('iam_role'),
    table='staging_songs'
    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_query=SqlQueries.songplay_table_insert,
    conn_id = 'redshift',
    table = 'songplays',
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table = 'users',
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='songs',
    sql_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='artists',
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id = 'redshift',
    table='time',
    sql_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = 'redshift',
    table = 'songplays',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator>>[stage_songs_to_redshift,stage_events_to_redshift]>>load_songplays_table>>[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table]>>run_quality_checks>>end_operator