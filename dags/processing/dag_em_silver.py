from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum

# Función para extraer credenciales de la conexión de Airflow
def get_aws_creds():
    conn = BaseHook.get_connection('aws_default')
    return conn.login, conn.password

access, secret = get_aws_creds()

# ... (tus imports y funciones get_aws_creds arriba) ...

with DAG(
    dag_id='transform_em_silver_v1',
    schedule='@daily',
    start_date=pendulum.datetime(2026, 4, 19, tz="UTC"),
    catchup=False,
    tags=['green-ai', 'silver', 'spark']
) as dag:

    # 1. El Sensor (4 espacios desde el borde)
    wait_for_bronze = ExternalTaskSensor(
        task_id='wait_for_bronze',
        external_dag_id='ingesta_em_bronze_SAFE_V4',
        external_task_id=None,
        check_existence=True,
        execution_date_fn=lambda dt: dt.replace(hour=0, minute=0, second=0, microsecond=0),
        mode='reschedule',
        poke_interval=60,
        timeout=3600
    )

    # 2. El Operador (4 espacios desde el borde)
process_silver = BashOperator(
        task_id='spark_silver_transform',
        env={
            'AWS_ACCESS_KEY': access or '',
            'AWS_SECRET_KEY': secret or '',
            'PYTHONPATH': '/home/airflow/.local/lib/python3.8/site-packages'
        },
        bash_command="""
        /home/airflow/.local/bin/spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY}" \
        --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_KEY}" \
        --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
        /opt/airflow/dags/processing/transform_em_silver.py \
        "s3a://green-ai-pf-bronze-a0e96d06/electricity_maps/zone=*/year={{ ds_nodash[:4] }}/month={{ ds_nodash[4:6] }}/day={{ ds_nodash[6:] }}/*.json" \
        "s3a://green-ai-pf-silver-a0e96d06/em_silver_table/"
        """
    )

# 3. La Dependencia (¡Debe estar al mismo nivel que wait_for_bronze!)
wait_for_bronze >> process_silver