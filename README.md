# AKVO Data Ingestion

This repository contains the scripts for ingesting the Akvo API data, storing it a postgres database with regular updates.

## Running locally

### Local Postgres Instance

You will need to have postgres running on your local machine or you can run it with [Docker-compose](https://docs.docker.com/compose/install):

```sh
docker-compose up -d 
# this will run in the background to stop it run:
docker-compose down
```

#### Inspecting the local database

You can inspect the local database using [pql](https://www.timescale.com/blog/how-to-install-psql-on-mac-ubuntu-debian-windows/) tool using the following command:

```sh

psql -h localhost -p 5432 -U postgres -W
```

The prompted password locally is `postgres`.

### Setup a python environment

```sh

python3 -m venv venv  # use python if it is a python > 3
source venv/bin/activate
```

### Install depencies

```sh
pip install -r requirements.txt
```

### Set up environment variables

Copy the `.env.template` file and name it `.env` you need to populate it with the values of the secrets (which can be found in lastpass under XXX)


