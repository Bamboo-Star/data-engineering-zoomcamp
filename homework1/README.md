This file contains homework answers, simple codes and explanations.


### Question 1. Understanding docker first run 

- 24.3.1

Dockerfile
```
FROM python:3.12.8
ENTRYPOINT [ "bash" ]
```

Bash
```
docker build -it test:pipversion .
docker run -it test:pipversion
pip --version
```


### Question 2. Understanding Docker networking and docker-compose

- postgres:5432
- db:5432

both "postgres:5432" and "db:5432" are correct
pg-admin connection can be setup using "postgres:5432" or "db:5432"


### Question 3. Trip Segmentation Count

- 104,802;  198,924;  109,603;  27,678;  35,189


### Question 4. Longest trip for each day

- 2019-10-31


### Question 5. Three biggest pickup zones
 
- East Harlem North, East Harlem South, Morningside Heights


### Question 6. Largest tip

- JFK Airport


### Question 7. Terraform Workflow

- terraform init, terraform apply -auto-approve, terraform destroy