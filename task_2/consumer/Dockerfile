FROM python:3.7


RUN pip install pandas kafka kafka-python json sqlalchemy psycopg2 flask-sqlalchemy 


WORKDIR /app
COPY task_2_consumer.py task_2_consumer.py 


ENTRYPOINT ["python","task_2_consumer.py"]