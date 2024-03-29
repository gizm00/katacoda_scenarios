export AIRFLOW__WEBSERVER__WORKER_REFRESH_BATCH_SIZE=0
export AIRFLOW__WEBSERVER__WORKER_REFRESH_INTERVAL=0
export AIRFLOW__WEBSERVER__WORKERS=2
sleep 3s
pip install -r requirements.txt
airflow db init
airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.org \
    --password password
airflow webserver --port 8080 -D
