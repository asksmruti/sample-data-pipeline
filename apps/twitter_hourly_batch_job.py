from datetime import timedelta
from textwrap import dedent
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Smruti',
    'depends_on_past': False,
    'email': ['smruti.sqa@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
        'twitter-batch',
        default_args=default_args,
        description='DAG to ingest data from twitter API into ',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        tags=['twitter'],
) as dag:

    t1 = BashOperator(
        task_id='twitter-batch-job',
        bash_command='date',
    )

    ingestion_lib_path = '${AIRFLOW_HOME}/dags/twitter/src/'

    t2 = BashOperator(
        task_id='twitter-feed-ingest',
        depends_on_past=False,
        bash_command=f'cd { ingestion_lib_path } && python3 twitter_s3_connector.py -s 10 -t keyword.txt'
    )

    dag.doc_md = __doc__

    t2.doc_md = dedent(
        """\
    Run twitter feed ingestion
    """
    )

    t3 = BashOperator(
        task_id='aws-glue-job',
        depends_on_past=False,
        bash_command=f'cd { ingestion_lib_path } && python3 run_glue_job.py -j twitter-etl',
    )
    t3.doc_md = dedent(
        """\
        Twitter data feed processing
    """
    )

    t1 >> t2 >> t3
