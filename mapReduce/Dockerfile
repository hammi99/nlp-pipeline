FROM python

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} 
RUN mkdir -p ${SPARK_HOME}


RUN apt-get update
RUN apt-get install openjdk-17-jdk -y

WORKDIR /
COPY . /app


WORKDIR /app/sparkFiles
# install spark
RUN tar xvzf spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install spark database connectors drivers
RUN mv jars/* ${SPARK_HOME}/jars

WORKDIR /app
RUN pip3 install -r requirements.txt

CMD jupyter lab 			    \
	--allow-root 				\
	--no-browser 				\
	--ServerApp.ip='0.0.0.0' 	\
	--ServerApp.token='' 		\
	--ServerApp.password=''

EXPOSE 8888