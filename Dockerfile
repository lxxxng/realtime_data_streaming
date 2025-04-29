FROM apache/airflow:2.9.0-python3.11 

# Install kafka-python and requests
RUN pip install kafka-python requests 
