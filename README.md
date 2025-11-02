1. Run following command. PLease see [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) for official documentation.
- `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.1/docker-compose.yaml'`
2. Make the base image using `Dockerfile.base` file
<br>**Command**: `docker build -t jeet/airflow-spark-base:latest -f Dockerfile.base .
`</br>
3. Now, make project-level `Dockerfile` which is  for small updates (no heavy deps).

**NOTES:**

1. Flow is usually `Dockerfile.base` -> `Dockerfile` -> `docker-compose.yml`
2. According to our needs, we can use/skip `Dockerfile`.
    - If we need Production like SETUP, we use `Dockerfile` in which we have COPY command. Then we do not mount any volumnes (for codes atleast). So wehataver changes we make, do not get written to PROD like image.
    - If we need Dev like structure, we can skip `Dockerfile` and use your base image `docker-compose.yml` file in which we have mount. This enables live code updates and local persistence.
3. Even if we use dev container, we still gonna need mounts to rfelect on local and vice versa.
4. why we run below manually? 

    `mkdir -p ./dags ./logs ./plugins ./config`<br>
    `echo -e "AIRFLOW_UID=$(id -u)" > .env`

    On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in dags, logs, config and plugins will be created with root user ownership. You have to make sure to configure them for the docker-compose. Basically, it might create permission issues.

For this, we will use dev like structure and skip `Dockerfile`