FROM apache/airflow:2.7.3
RUN pip install sodapy python-dotenv google-cloud-storage google-cloud-bigquery apache-airflow-providers-google