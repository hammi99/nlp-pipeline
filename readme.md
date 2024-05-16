 1. install docker desktop 
 2. start docker desktop 
 3. in docker desktop, go to settings -> resources, increase swap to 4gb, ram to 4gb, cpu to 100% and then click `apply and restart`   
 3. open terminal 
 4. go to the project's "src" directory, where "docker-compose.yml" file is located 
 5. execute command `docker compose up -d --build` in the terminal, this will build and deploy all the docker containers
 6. execute the command `docker exec source-db sh -c "mysql --local-infile=1 -u root -ppassword < /app/sql/2-dump.sql"`, this will add your csv data to source-db (mysql)
 7. map-reduce notebook is located at http://127.0.0.1:8888/lab
 8. open it and execute the contents, this will preprocess the data and add it to sink-db (mongodb)
 9. analytics notebook is located at http://127.0.0.1:8889/lab
10. open it and execute the contents, this will perform analysis on the preprocessed data and write it to sink-db 
