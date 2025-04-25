from airflow.models import BaseOperator, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Custom Operator to load JSON files from S3 into Amazon Redshift.

    Features:
    - Uses Airflow Variables for S3 bucket
    - Uses IAM Role or AWS credentials
    - Supports 'auto' or custom json_path
    - Supports templated s3_key (for backfills)
    - Optional table truncation (idempotent)
    """

    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_key,
                 json_path=None,
                 region='us-west-2',
                 truncate_table=True,
                 iam_role=None,
                 file_format='JSON',
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.truncate_table = truncate_table
        self.iam_role = iam_role
        self.file_format = file_format.upper()

    def execute(self, context):
        self.log.info(f"Staging data from S3 to Redshift table: {self.table}")

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Get AWS credentials
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_credentials_id, client_type='sts')
        credentials = aws_hook.get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key

        # Get S3 bucket from Airflow Variable
        bucket_name = Variable.get("s3_bucket")

        # Resolve S3 path
        rendered_key = self.s3_key  # Already templated by Airflow
        s3_path = f"s3://{bucket_name}/{rendered_key}"

        # Truncate table before load (idempotency)
        if self.truncate_table:
            self.log.info(f"Truncating data in destination Redshift table {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        # Resolve credentials block
        if self.iam_role:
            credentials_string = f"IAM_ROLE '{self.iam_role}'"
        else:
            credentials_string = f"ACCESS_KEY_ID '{access_key}' SECRET_ACCESS_KEY '{secret_key}'"

        # Handle JSON path
        if not self.json_path or self.json_path.strip().lower() == 'auto':
            json_path_clause = "'auto'"
        else:
            json_path_clause = f"'s3://{bucket_name}/{self.json_path}'"

        # Final COPY SQL
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            {credentials_string}
            REGION '{self.region}'
            FORMAT AS JSON {json_path_clause}
            TIMEFORMAT AS 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        """

        self.log.info(f"Executing COPY command:\n{copy_sql}")
        redshift.run(copy_sql)
        self.log.info(f"Successfully staged {self.table} from {s3_path}")