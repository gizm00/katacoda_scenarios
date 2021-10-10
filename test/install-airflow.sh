sleep 4s
pip install -r requirements.txt --ignore-installed
airflow db init
airflow users create \
    --username admin \
    --firstname Firstname \
    --lastname Lastname \
    --role Admin \
    --email admin@example.org \
    --password password
airflow webserver --port 8080 -D
