FROM python:3.9


RUN pip install pandas 
RUN pip install kafka-python 


WORKDIR /app
COPY task_2_producer.py task_2_producer.py 


ENTRYPOINT ["python","task_2_producer.py"]