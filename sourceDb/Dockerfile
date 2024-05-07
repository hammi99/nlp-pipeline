FROM mysql

ENV MYSQL_ROOT_PASSWORD password
ENV MYSQL_DATABASE default

# COPY sql/* /docker-entrypoint-initdb.d
COPY sql/1-init.sql /docker-entrypoint-initdb.d
# COPY sql/2-dump.sql /docker-entrypoint-initdb.d

# COPY data/* /var/lib/mysql-files
COPY . ./app
EXPOSE 3306