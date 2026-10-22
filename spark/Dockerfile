FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root

# JDBC driver để Spark đọc OLTP / ghi DWH (PostgreSQL)
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN curl -fsSL -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    && chmod 644 /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# psycopg2: driver Python dùng để publish stg -> dwh trong 1 transaction
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10

# Data lake (bronze layer) — named volume kế thừa quyền sở hữu này
RUN mkdir -p /opt/lake && chown -R spark:spark /opt/lake

COPY conf/spark-defaults.conf conf/log4j2-driver.properties /opt/spark/conf/
COPY submit.sh /opt/submit.sh
RUN chmod 755 /opt/submit.sh

ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1

USER spark
WORKDIR /opt/spark/work-dir
