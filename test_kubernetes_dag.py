from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.kubernetes_pod_operator import \
    KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

DOCKER_IMAGE = "329245343759.dkr.ecr.us-west-2.amazonaws.com/dispatchhealth-bi/airflow-test:latest"
# Name your dag. This will be what the Airflow UI displays.
DAG_NAME = 'test_kubernetes_dag'
# Version your dag.
# I have found it best practice to version the Dag so you can be sure the Dag
# that is running is the dag you think it is.
DAG_VERSION = '0.0.1'
EMAIL = 'benjamin.everett@dispatchhealth.com'

# See docs: https://airflow.apache.org/tutorial.html#default-arguments
# And BaseOperator docs: https://airflow.apache.org/_api/airflow/models/index.html#airflow.models.BaseOperator
default_args = {
    # This can be any owner
    'owner': 'cloud-composer-tutorial',
    # I have not played with this variable yet
    'depends_on_past': False,
    # Set the start date.
    # As an aside, if you set a start date in the past and tell Airflow to run
    # once a day, it will run a task for each day in order to catch up with the
    # present
    # Docs: https://airflow.apache.org/timezone.html
    'start_date': datetime.utcnow(),
    # Set email settings. I have not played with this much, but to this point
    # in time, I have not been able to get it to work
    # Perhaps this article will help: https://danvatterott.com/blog/2018/08/29/custom-email-alerts-in-airflow/
    'email': [EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    # If a task fails, how many times will it retry.
    'retries': 1,
    # How long until a task retries
    'retry_delay': timedelta(seconds=30),
    # We definitely want the logs. You can also look at logs in the kubernetes
    # pod
    'get_logs': True,
    # A new pod is spun up for each task you run. Setting this to 'True' will
    # delete the pod after it runs. Regardless of whether the task ran
    # successfully or not. Set to 'False' when you're trying to debug. But
    # remember to delete your pods!!
    'is_delete_operator_pod': True
}

# Docs: https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator#secret-config
secret_env_test = secret.Secret(
    # Expose the secret as environment variable.
    deploy_type='env',
    # Specify the name of the environment variable for the pod
    deploy_target='TEST_ENV',
    # Name of the Kubernetes Secret from which to pull the environment variable
    secret='test-env',
    # Key of the secret stored in the Kubernetes Secret object
    key='TEST')

# secret_storage = secret.Secret(
#     # Expose the secret as volume.
#     deploy_type='volume',
#     # Specify the path to the folder (not the full filepath) to store the secret
#     deploy_target='/etc/storage-credentials',
#     # Name of the Kubernetes Secret from which to pull the volume
#     secret='storage-admin',
#     # Name of the secret stored in the Kubernetes Secret object
#     # Incidentally, this will be become the filename of the secret.
#     # I.e. the full filepath will be /etc/storage-credentials/gcs-admin-key.json
#     key='gcs-admin-key.json')

# Instantiate your dag with the default args set above.
dag = DAG(
    f"{DAG_NAME}_{DAG_VERSION}",
    # Docs: https://airflow.apache.org/tutorial.html#instantiate-a-dag
    default_args=default_args,
    # 'schedule_interval' should be a cron string.
    # For debugging purposes I like to set it to "@once" so the job runs
    # one time, immediately. Imagine that!
    # Docs: https://airflow.apache.org/scheduler.html#dag-runs
    schedule_interval="@once"
    )

# Start with something simple
# Docs: https://airflow.apache.org/_modules/airflow/operators/dummy_operator.html
start = DummyOperator(
    task_id='start',
    dag=dag
)
# Airflow General Docs: https://airflow.apache.org/kubernetes.html
# Airflow KubernetesPodOperators specific Docs: https://airflow.apache.org/_api/airflow/contrib/operators/kubernetes_pod_operator/index.html
# Google Docs: https://cloud.google.com/composer/docs/how-to/using/using-kubernetes-pod-operator
# Specifcy defaults for Pod Operator
default_pod = {
    # Pod must have a namespace to run within
    'namespace':'default',
    # Specify which docker image to pull
    'image':DOCKER_IMAGE,
    # Labels for the pod
    'labels':{"pipeline": "test"},
    # Instantiate the K8s Pod Operator to point to the Dag 'dag'
    'dag': dag
}

# I have not yet figured out all the places that Airflow uses 'name'
# as opposed to 'task_id'. What I do know is that names cannot have '_'
# but instead '-' are acceptable.
# Thus, I have adopted the convention of using '-' in names and '_' in tasks
# in order to track where Airflow differentiates between the two.
# Docs: https://airflow.apache.org/_api/airflow/contrib/operators/kubernetes_pod_operator/index.html
hello_world = KubernetesPodOperator(
    **default_pod,
    name="hello-world",
    task_id="hello_world",
    image_pull_policy="Always",
    cmds=["python3"],
    arguments=["hello_docker_world.py"]
)
# Docs: https://airflow.apache.org/concepts.html?#operators
"""
From the KubernetesPodOperator Docs:
    xcom_push (bool) – If xcom_push is True, the content of the file
    /airflow/xcom/return.json in the container will also be pushed to an XCom
    when the container completes.
"""
test_push_xcom = KubernetesPodOperator(
    **default_pod,
    name="test-push-xcom",
    task_id="test_push_xcom",
    xcom_push=True,
    cmds=["python3"],
    arguments=["test_xcom_push.py"]
)
# Two different ways to do xcom values
# The first:
def pull_function(**kwargs):
    ti = kwargs['ti']
    ls = ti.xcom_pull(task_ids='test-push-xcom-task')
    print(ls)
test_pull = PythonOperator(
    task_id='test_xcom_pull',
    dag=dag,
    python_callable=pull_function,
    provide_context=True
)
# The second
# Docs: https://airflow.apache.org/tutorial.html#templating-with-jinja
XCOM_VALS = "{{ task_instance.xcom_pull(task_ids='test_push_xcom', key='return_value')['test_xcom'] }}"

test_xcom_vals_file = KubernetesPodOperator(
    **default_pod,
    name="test-xcom-vals-file",
    task_id="test_xcom_vals_file",
    env_vars={'test_xcom': XCOM_VALS},
    cmds=["python3"],
    arguments=["test_xcom_val.py"]
)

test_env_vars = KubernetesPodOperator(
    **default_pod,
    name="test-env-vars",
    task_id="test_env_vars",
    secrets=[secret_env_test],
    cmds=["python3"],
    arguments=["test_env_vars.py"]
)

# test_gcs_buckets = KubernetesPodOperator(
#     **default_pod,
#     name="test-gcs-buckets",
#     task_id="test_gcs_buckets",
#     secrets=[secret_storage],
#     env_vars={'GOOGLE_APPLICATION_CREDENTIALS': '/etc/storage-credentials/gcs-admin-key.json'},
#     cmds=["python3"],
#     arguments=["test_gcs_buckets.py"]
# )

# secret_cloud_sql = secret.Secret(
#     # Expose the secret as environment variable.
#     deploy_type='env',
#     # Specify the name of the environment variable for this pod
#     deploy_target='DB_PASSWORD',
#     # Name of the Kubernetes Secret
#     secret='test-env',
#     # Key of the secret stored in the Kubernetes Secret object
#     key='TEST_DB_PASSWORD')
#
# test_cloud_sql = KubernetesPodOperator(
#     **default_pod,
#     name="test-cloud-sql",
#     task_id="test_cloud_sql",
#     secrets=[secret_cloud_sql],
#     cmds=["python3"],
#     arguments=["test_cloud_sql.py"]
# )
# Finish with something simple
finish = DummyOperator(
    name='finish',
    task_id='finish',
    dag=dag)

# Docs: https://airflow.apache.org/tutorial.html#setting-up-dependencies
# These can be written two ways.
# Like this:

# start >> hello_world >> test_env_vars >> test_gcs_buckets
# test_push_xcom >> [test_xcom_vals_file, test_pull]
# finish << test_gcs_buckets
# finish << test_xcom_vals_file

# Or like this:
start.set_downstream(hello_world)
hello_world.set_downstream(test_env_vars)
# test_env_vars.set_downstream(test_gcs_buckets)
# finish.set_upstream(test_gcs_buckets)

test_push_xcom.set_downstream([test_xcom_vals_file, test_pull])
finish.set_upstream(test_xcom_vals_file)
