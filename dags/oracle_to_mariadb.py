import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

default_args = {
    'email_on_failure': True,
    'email': ['kenneth.lieyanto@suryapamenang.com'],
    'email_on_retry': False,
}

with DAG(
    dag_id="oracle_to_mariadb",
    description="Daily Oracle EBS → MariaDB full refresh",
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 26, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "oracle-to-mariadb"],
    default_args=default_args,
) as dag:

    KubernetesPodOperator(
        task_id="run_transfer",
        name="oracle-to-mariadb-transfer",
        namespace="airflow",
        image="ghcr.io/kennethl-sp/oracle-to-mariadb:staging",
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(name="oracle-to-mariadb-config")
            ),
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name="oracle-to-mariadb-secrets")
            ),
        ],
        env_vars={"QUERY_FILE_PATH": "/etc/query/query.sql"},
        volumes=[
            k8s.V1Volume(
                name="query-volume",
                config_map=k8s.V1ConfigMapVolumeSource(name="oracle-to-mariadb-query"),
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="query-volume", mount_path="/etc/query", read_only=True)
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
    )

