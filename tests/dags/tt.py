from kubernetes.client import models as k8s
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "jkwak",
    "depends_on_past": False,
    "email": ["jkwak@shinhan.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


task_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                image="192.168.27.193:5000/shai-am-dev:v0.2",
                image_pull_policy="Always",
            )
        ],
    )
)


@dag(default_args=default_args, start_date=days_ago(2), schedule_interval=None)
def jkwak_resource_dag():  # python function name acting as the DAG identifer
    """
    def func1():
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.datasets import make_classification

        X, y = make_classification(n_samples=1000, n_features=4, n_informative=2, n_redundant=0, random_state=0, shuffle=False)
        clf = RandomForestClassifier(max_depth=2, random_state=0)
        clf.fit(X, y)
        # time.sleep(30)
        return clf
    """
    # task1 = PythonOperator(task_id="task_1", python_callable=func1, executor_config={"pod_override": task_pod})
    task1 = KubernetesPodOperator(
        task_id="task-1",
        name="task-1",
        namespace="shai-am",
        image="192.168.27.193:5000/shai-am-dev:v0.2",
        image_pull_policy="Always",
        # image_pull_secrets=[k8s.V1LocalObjectReference("image_credential")],
        get_logs=True,
        is_delete_operator_pod=False,
        cmds=["python"],
    )
    print(task1)


dag = jkwak_resource_dag()  # DAG is declared
