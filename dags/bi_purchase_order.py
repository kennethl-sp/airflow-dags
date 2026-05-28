import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

with DAG(
    dag_id="bi_purchase_order",
    description="Untuk BI Purchase Order. Dipakai marketing.",
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 26, tz="Asia/Jakarta"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "bi_purchase_order"],
    on_failure_callback=[
         send_smtp_notification(
            from_email="no-reply@suryapamenang.com",
            to="kenneth.lieyanto@suryapamenang.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
        )
    ],
) as dag:
    KubernetesPodOperator(
        task_id="move",
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

