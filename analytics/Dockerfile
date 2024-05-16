FROM python:3.8

ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

RUN mkdir -p ${HADOOP_HOME} 
RUN mkdir -p ${SPARK_HOME}

# install the nightmare, called "java 8"
RUN apt-get update --yes
RUN apt-get install --yes gnupg2 apt-transport-https
RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A1EAC8B7
RUN echo 'deb https://rpardini.github.io/adoptium-deb-installer stable main' > /etc/apt/sources.list.d/rpardini-adoptium.list
RUN apt-get update --yes
RUN apt-get install --yes adoptium-8-installer



WORKDIR /
COPY . /app

# install spark
# RUN curl https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz -o spark.tgz
# RUN tar -x -f spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install mongodb spark connector
# RUN curl https://repo1.maven.org/maven2/org/mongodb/mongodb-jdbc/2.1.2/mongodb-jdbc-2.1.2-all.jar -o ${SPARK_HOME}/jars/mongodb-jdbc-2.1.2-all.jar

# install spark
WORKDIR /app/sparkFiles
RUN tar -x -f spark.tgz --directory ${SPARK_HOME} --strip-components 1
# install spark database connectors drivers
RUN mv jars/* ${SPARK_HOME}/jars


WORKDIR /app
# RUN pip3 install -r requirements.txt
RUN pip3 install jupyterlab
# RUN pip install pyspark
# RUN pip install sparknlp



# start jupyter server
CMD jupyter lab 				\
	--allow-root 				\
	--no-browser 				\
	--ServerApp.ip='0.0.0.0' 	\
	--ServerApp.port=8889 		\
	--ServerApp.token='' 		\
	--ServerApp.password=''


EXPOSE 8889