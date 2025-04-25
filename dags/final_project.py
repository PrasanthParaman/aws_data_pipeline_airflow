from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.check_tables_exist import CheckTablesExistOperator
from final_project_operators.custom_table_create import CustomSqlOperator
from udacity.common.final_project_sql_statements import SqlQueries
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner': 'Divya Prasanth Paraman',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_tables = CustomSqlOperator(task_id='create_tables',
                        postgres_conn_id='redshift')

    check_table_exists = CheckTablesExistOperator(task_id='check_table_exists',
                        postgres_conn_id='redshift',
                        tables=['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time'])

    stage_events = StageToRedshiftOperator(
        task_id='stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_key='log-data/{{ logical_date.strftime("%Y") }}/{{ logical_date.strftime("%m") }}/{{ logical_date.strftime("%Y-%m-%d") }}-events.json',
        json_path='log_json_path.json',  # or your S3 path like 's3://my-bucket/log_json_path.json'
        region='us-east-1',
        truncate_table=True,
        iam_role=''
        )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_key='song-data/A/A/',
        json_path='auto',  # or your S3 path like 's3://my-bucket/log_json_path.json'
        region='us-east-1',
        truncate_table=True,
        iam_role=''
    )

    load_fact_table_songplays = LoadFactOperator(
        task_id='load_fact_table_songplays',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='load_user_dimension_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert,
        mode='truncate-insert'
    )
    
    load_song_dimension_table = LoadDimensionOperator(
        task_id='load_song_dimension_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert,
        mode='truncate-insert'
    )
  
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dimension_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert,
        mode='truncate-insert'
    )
  
    load_time_dimension_table = LoadDimensionOperator(
        task_id='load_time_dimension_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert,
        mode='truncate-insert'
    )

    run_quality_checks_staging = DataQualityOperator(
        task_id='Run_data_quality_checks_staging',
        conn_id='redshift',
        tables=['staging_events', 'staging_songs']
    )

    run_quality_checks_fact_dim_tables = DataQualityOperator(
        task_id='run_quality_checks_fact_dim_tables',
        conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    stop_operator = DummyOperator(task_id='stop_execution')

    start_operator >> create_tables >> check_table_exists 
    check_table_exists >> (stage_events, stage_songs_to_redshift) >> run_quality_checks_staging
    run_quality_checks_staging >> load_fact_table_songplays >> (load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table) >> run_quality_checks_fact_dim_tables
    run_quality_checks_fact_dim_tables >> stop_operator

final_project_dag = final_project()