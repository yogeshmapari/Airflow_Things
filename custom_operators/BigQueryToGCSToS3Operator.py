from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
import pandas as pd
import tempfile
from boto3.s3.transfer import TransferConfig
from jinja2 import Template

class BigQueryToGCSToS3Operator(BaseOperator):
    """
    Below is the description of the respective parameters of the operator:

    :param sql_query: The actual SQL query whose data will be exported to the file.
    :param parameters: These are the dynamic SQL parameters used for replacing values according to the environment.
    :param gcs_bucket=None: The bucket where we need to store the actual data file in GCS.
    :param gcs_path=None: The path where we need to store the actual data file in GCS.
    :param project_id=None: The BigQuery project ID to be provided.
    :param region=None: The region required for establishing a BigQuery connection.
    :param output_format="csv"/"json"/"parquet": The file format for the exported data.
    :param csv_delimiter=",": The delimiter for CSV files; the default is a comma, but it can be customized as required.
    :param header=True: Controls whether the file includes a header row.
    :param quote_symbol=None: Specifies the symbol used to quote data in the destination file.
    :param s3_transfer=False/True: Determines whether to move the data to S3.
    :param s3_bucket=None: The S3 bucket where the file will be transferred.
    :param s3_key=None: The S3 path where the file will be stored.
    :param gcp_conn_id="google_cloud_default": The GCP connection ID.
    :param aws_conn_id="aws_default": The AWS connection ID.
    :param *args: Additional arguments.

    Designed by Yogesh Mapari
    Date: 23-01-2025
    """

    @apply_defaults
    def __init__(
        self,
        sql_query,
        parameters=None,
        gcs_bucket=None,
        gcs_path=None,
        dataplex_project_id=None,
        region=None,
        output_format="csv",
        csv_delimiter=",",
        header=True,
        quote_symbol=None,
        s3_transfer=False,
        s3_bucket=None,
        s3_key=None,
        gcp_conn_id="google_cloud_default",
        aws_conn_id="aws_default",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.parameters = parameters or {}
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.dataplex_project_id = dataplex_project_id
        self.region = region
        self.output_format = output_format
        self.csv_delimiter = csv_delimiter
        self.header = header
        self.quote_symbol = quote_symbol
        self.s3_transfer = s3_transfer
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.gcp_conn_id = gcp_conn_id
        self.aws_conn_id = aws_conn_id

    def render_query(self):
        """
        Replaces placeholders in the SQL query template with values from `parameters`.

        :return: The rendered SQL query.
        """
        self.log.info("Rendering the SQL query with parameters...")

        try:
            template = Template(self.sql_query)
            rendered_query = template.render(params=self.parameters)
            self.log.info("Query successfully rendered using Jinja2.")
            return rendered_query
        except KeyError as e:
            raise ValueError(f"Missing parameter in parameters: {e}")
        except Exception as e:
            raise ValueError(f"Error rendering query: {str(e)}")

    def execute(self, context):
        # Step 1: Render the query with parameters
        rendered_query = self.render_query()

        # Step 2: Fetch data from BigQuery
        self.log.info("Fetching data from BigQuery...")
        bq_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id, location=self.region, use_legacy_sql=False
        )
        bq_client = bq_hook.get_client(
            project_id=self.dataplex_project_id, location=self.region
        )

        query_job = bq_client.query(rendered_query)
        results = query_job.result()
        # Convert the results to a pandas DataFrame
        data = [dict(row) for row in results]
        df = pd.DataFrame(data)

        # Step 3: Write the data to GCS
        self.log.info("Writing data to GCS...")
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        with tempfile.NamedTemporaryFile(
            delete=True, suffix=f".{self.output_format}"
        ) as temp_file:
            # Save the data to the file
            if self.output_format == "csv":
                if isinstance(self.quote_symbol, str):
                    df = df.applymap(
                        lambda x: f"{self.quote_symbol}{x}{self.quote_symbol}"
                    )
                    df.to_csv(
                        temp_file.name,
                        index=False,
                        sep=self.csv_delimiter,
                        header=self.header,
                    )
                else:
                    df.to_csv(
                        temp_file.name,
                        index=False,
                        sep=self.csv_delimiter,
                        header=self.header,
                    )
            elif self.output_format == "json":
                df.to_json(temp_file.name, orient="records", lines=True)
            elif self.output_format == "parquet":
                df.to_parquet(temp_file.name, index=False)
            else:
                raise ValueError(f"Unsupported output format: {self.output_format}")

            # Upload the file to GCS
            gcs_hook.upload(
                bucket_name=self.gcs_bucket,
                object_name=self.gcs_path,
                filename=temp_file.name,
            )
            self.log.info(
                f"File written to GCS: gs://{self.gcs_bucket}/{self.gcs_path}"
            )
            # Step 4: Transfer the data to S3 (if required)
            if self.s3_transfer:
                self.log.info("Transferring data to S3...")
                if not self.s3_bucket or not self.s3_key:
                    raise ValueError(
                        "s3_bucket and s3_key must be provided when s3_transfer is True."
                    )
                s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
                s3_client = s3_hook.get_conn()
                s3_client.upload_file(
                    Filename=temp_file.name,
                    Bucket=self.s3_bucket,
                    Key=self.s3_key,
                    Config=TransferConfig(use_threads=False),
                )
                self.log.info(
                    f"File transferred to S3: s3://{self.s3_bucket}/{self.s3_key}"
                )
