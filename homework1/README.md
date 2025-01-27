This file contains simple codes and explanations.


# Question 1. Understanding docker first run 
Dockerfile:
    FROM python:3.12.8

    ENTRYPOINT [ "bash" ]

Bash:
    docker build -it test:pipversion .
    docker run -it test:pipversion
    pip --version


# Question 2. Understanding Docker networking and docker-compose

both "postgres:5432" and "db:5432" are correct
pg-admin connection can be setup using "postgres:5432" or "db:5432"