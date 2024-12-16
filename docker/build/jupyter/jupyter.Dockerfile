FROM jupyter/pyspark-notebook:spark-3.5.0
WORKDIR /
COPY ./jupyter/jupyter_requirements.txt /
RUN pip install -r jupyter_requirements.txt
RUN chown -R jovyan home/jovyan


ENV CHOWN_HOME=yes
ENV CHOWN_HOME_OPTS='-R'

USER root
