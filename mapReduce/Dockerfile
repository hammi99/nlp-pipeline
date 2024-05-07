FROM python

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} 
RUN mkdir -p ${SPARK_HOME}


RUN apt-get update
RUN apt-get install openjdk-17-jdk -y

WORKDIR /
COPY . /app
# RUN mkdir /app

# install spark
WORKDIR /app/sparkFiles
RUN tar xvzf spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install spark database connectors drivers
# RUN mv mysql-connector-j-8.4.0.jar ${SPARK_HOME}/jars
# RUN mv mongo-spark-connector_2.12-10.3.0-all.jar ${SPARK_HOME}/jars
RUN mv jars/* ${SPARK_HOME}/jars

WORKDIR /app
RUN pip3 install -r requirements.txt

# CMD jupyter notebook --allow-root --no-browser --ip=0.0.0.0
# CMD jupyter notebook --allow-root --no-browser --ip=0.0.0.0 --NotebookApp.token='token'
# CMD jupyter notebook --no-browser --allow-root \
# 	--IdentityProvider.token='yourSecretToken' \
# 	--ServerApp.allow_origin='*'               \
# 	--ServerApp.allow_remote_access=True       \
# 	--ServerApp.ip='0.0.0.0'                   \
# 	--ServerApp.port=9999
	# --ServerApp.port=8888

CMD jupyter notebook --allow-root --ip='0.0.0.0'

EXPOSE 8888