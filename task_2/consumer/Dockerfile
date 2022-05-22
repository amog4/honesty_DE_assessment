FROM python:3.8


RUN pip install pandas d
RUN pip install kafka-python
RUN pip install sqlalchemy
RUN pip install psycopg2
RUN pip install flask-sqlalchemy 


WORKDIR /app
COPY task_2_consumer.py task_2_consumer.py 


ENTRYPOINT ["python","task_2_consumer.py"]