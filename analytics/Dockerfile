FROM python

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} 
RUN mkdir -p ${SPARK_HOME}


RUN apt-get update
RUN apt-get install openjdk-17-jdk -y

WORKDIR /
COPY . /app

# install spark
# RUN curl https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz -o spark.tgz
# RUN tar -x -f spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install mongodb spark connector
# RUN curl https://repo1.maven.org/maven2/org/mongodb/mongodb-jdbc/2.1.2/mongodb-jdbc-2.1.2-all.jar -o ${SPARK_HOME}/jars/mongodb-jdbc-2.1.2-all.jar

WORKDIR /app/sparkFiles
# install spark
RUN tar xvzf spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install spark database connectors drivers
RUN mv jars/* ${SPARK_HOME}/jars


WORKDIR /app
RUN pip3 install -r requirements.txt



CMD jupyter lab					\
	--allow-root 				\
	--no-browser 				\
	--ServerApp.ip='0.0.0.0' 	\
	--ServerApp.port=8889 		\
	--ServerApp.token='' 		\
	--ServerApp.password=''

EXPOSE 8889

