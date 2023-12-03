FROM python:3.7

RUN pip install apache-airflow[postgres]==1.10.15
RUN pip install SQLAlchemy==1.3.23
RUN pip install markupsafe==2.0.1
RUN pip install wtforms==2.3.3

RUN mkdir /project
COPY scripts/ /project/scripts/
COPY config/airflow.cfg /usr/local/airflow/airflow.cfg

RUN chmod +x /project/scripts/init.sh

ENTRYPOINT ["/project/scripts/init.sh"]