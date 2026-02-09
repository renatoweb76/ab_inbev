FROM apache/airflow:2.7.1-python3.9

USER root

# 1. Instala Java e Ferramentas
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ant && \
    apt-get clean;

# 2. Define JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

# 3. TRUQUE: Cria um link simbólico do Airflow para uma pasta global
# Isso garante que o comando 'airflow' funcione mesmo se o PATH do usuário quebrar
RUN ln -s /home/airflow/.local/bin/airflow /usr/bin/airflow

USER airflow

# 4. Instala as bibliotecas Python
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark \
    pandas \
    psycopg2-binary

# 5. Validação durante o build (se falhar aqui, nem sobe)
RUN airflow version