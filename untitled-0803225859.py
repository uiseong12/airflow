from airflow import DAG

from airflow_notebook.pipeline import NotebookOp
from airflow.utils.dates import days_ago

# Setup default args with older date to automatically trigger when uploaded
args = {
    "project_id": "untitled-0803225859",
}

dag = DAG(
    "untitled-0803225859",
    default_args=args,
    schedule_interval="@once",
    start_date=days_ago(1),
    description="Created with Elyra 2.2.4 pipeline editor using untitled.pipeline.",
    is_paused_upon_creation=False,
)


notebook_op_c2a8c21c_7108_4f7d_9b22_04215fad13f6 = NotebookOp(
    name="hello",
    namespace="ml-workshop",
    task_id="hello",
    notebook="Machine-Learning-on-Kubernetes/Chapter07/model_deploy_pipeline/hello.py",
    cos_endpoint="http://minio-ml-workshop:9000/",
    cos_bucket="airflow",
    cos_directory="untitled-0803225859",
    cos_dependencies_archive="hello-c2a8c21c-7108-4f7d-9b22-04215fad13f6.tar.gz",
    pipeline_outputs=[],
    pipeline_inputs=[],
    image="https://quay.io/ml-on-k8s/kaniko-container-builder:1.0.0",
    in_cluster=True,
    env_vars={
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
    },
    config_file="None",
    dag=dag,
)

notebook_op_c2a8c21c_7108_4f7d_9b22_04215fad13f6.image_pull_policy = "IfNotPresent"


notebook_op_5d2ce28a_6184_452e_afc4_a2726dcc2f64 = NotebookOp(
    name="world",
    namespace="ml-workshop",
    task_id="world",
    notebook="Machine-Learning-on-Kubernetes/Chapter07/model_deploy_pipeline/world.py",
    cos_endpoint="http://minio-ml-workshop:9000/",
    cos_bucket="airflow",
    cos_directory="untitled-0803225859",
    cos_dependencies_archive="world-5d2ce28a-6184-452e-afc4-a2726dcc2f64.tar.gz",
    pipeline_outputs=[],
    pipeline_inputs=[],
    image="https://quay.io/ml-on-k8s/airflow-python-runner:0.0.11",
    in_cluster=True,
    env_vars={
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "ELYRA_ENABLE_PIPELINE_INFO": "True",
    },
    config_file="None",
    dag=dag,
)

notebook_op_5d2ce28a_6184_452e_afc4_a2726dcc2f64.image_pull_policy = "IfNotPresent"

(
    notebook_op_5d2ce28a_6184_452e_afc4_a2726dcc2f64
    << notebook_op_c2a8c21c_7108_4f7d_9b22_04215fad13f6
)
