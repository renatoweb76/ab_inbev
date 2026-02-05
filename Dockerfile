FROM apache/airflow:2.7.1-python3.9

USER root

# Instala o OpenJDK-11 (Obrigatório para o Spark funcionar)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Define a variável JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Instala o provedor Spark para Airflow, o PySpark e bibliotecas comuns de dados
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark \
    pandas \
    psycopg2-binary