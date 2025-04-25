from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

class CustomSqlOperator(BaseOperator):
    """
    Custom Operator that executes multiple SQL statements from a provided SQL string.
    """

    sql_string = """
    DROP TABLE IF EXISTS staging_events;
    DROP TABLE IF EXISTS staging_songs;

    CREATE TABLE staging_events (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              CHAR(1),
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            TEXT,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              VARCHAR
    );

    CREATE TABLE staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     TEXT,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
    );

    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id         VARCHAR     PRIMARY KEY,
        start_time          TIMESTAMP   NOT NULL,
        userid              VARCHAR,
        level               VARCHAR,
        song_id             VARCHAR,
        artist_id           VARCHAR,
        sessionid           INT,
        location            TEXT,
        useragent           TEXT
    );

    CREATE TABLE IF NOT EXISTS users (
        userid              VARCHAR     PRIMARY KEY,
        first_name          VARCHAR,
        last_name           VARCHAR,
        gender              CHAR(1),
        level               VARCHAR
    );

    CREATE TABLE IF NOT EXISTS songs (
        song_id             VARCHAR     PRIMARY KEY,
        title               VARCHAR,
        artist_id           VARCHAR,
        year                INT,
        duration            FLOAT
    );

    CREATE TABLE IF NOT EXISTS artists (
        artist_id           VARCHAR     PRIMARY KEY,
        name                VARCHAR,
        location            TEXT,
        latitude            FLOAT,
        longitude           FLOAT
    );

    CREATE TABLE IF NOT EXISTS time (
        start_time          TIMESTAMP   PRIMARY KEY,
        hour                INT,
        day                 INT,
        week                INT,
        month               INT,
        year                INT,
        weekday             INT
    );
    """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 *args, **kwargs):
        super(CustomSqlOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql_string = CustomSqlOperator.sql_string

    def execute(self, context):
        self.log.info(f"Connecting to Redshift using connection ID: {self.postgres_conn_id}")
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info("Splitting SQL string into individual statements.")
        sql_statements = self.sql_string.split(';')

        for statement in sql_statements:
            cleaned_statement = statement.strip()
            if cleaned_statement:
                self.log.info(f"Executing SQL: {cleaned_statement[:100]}...")  # Preview first 100 chars
                redshift.run(cleaned_statement)

        self.log.info("All SQL statements executed successfully.")