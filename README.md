# Introduction

This project read pickled blobs from stdin an write them in a storage that should handle 1TB of
data.

It is NOT INTENDED for production.

# Example of running environment

An easy setup is to use a virtualenv for python dependancies and a Docker container to get the
postgreSQL running database.
Tests has been run with Python 3.14.2 and Podman 5.7.1. In this example, the `podman`
commands should be `docker` compatible.

The Docker registry used in this example is https://docker.io/library/postgres

## Running the postgreSQL database
```
CONTAINER=podman
DB_CONTAINER_NAME=storage
DB_NAME=storage
DB_PARTICLE_TABLE_NAME=particle
#FIXME use a secure way to store the password (and use it)
DB_PASS=Vinyl3Coffin
DB_PORT=5432

$CONTAINER run -d \
    --name "$DB_CONTAINER_NAME" \
    -e POSTGRES_PASSWORD="$DB_PASS" \
    -e POSTGRES_DB="$DB_NAME" \
    -e POSTGRES_USER="USER" \
    -p 5432:$DB_PORT \
    postgres
```

After the container has been downloaded and started, we might test it for open connection
```
$CONTAINER exec $DB_CONTAINER_NAME pg_isready -d $DB_NAME
```
> /var/run/postgresql:5432 - accepting connections

## Python environment setup
```
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
```

## Running the example
### Generating data and store in the DB
```
. .venv/bin/activate
python data_generator.py --pps 150 --max-mb 500 | python particle_storage.py write
```
> First Data timestamps 2026-02-23T15:44:25.976701+00:00
> Reached 524359144 bytes after 172 packets (85226 particles). Stopping.
> Write bandwidth: 0.56 kParticles/s
> Last Data timestamp 2026-02-23T15:46:54.271702+00:00
> Wrote to storage 172 packets (524359144 bytes).

### Reading back data with a timestamp range
```
. .venv/bin/activate
python particle_storage.py read --start "2026-02-23T15:44:25.976701+00:00" --stop "2026-02-23T15:46:00.271702+00:00"
```
> Reading data between 2026-02-23 15:44:25.976701+00:00 and 2026-02-23 15:46:00.271702+00:00
> Found 54016 particles
> Read bandwidth 9.99 kParticles/s.

# Limitation

* The podman command should set a fixed named volume with enough storage capacity
* The reading data implementation is only partial (not base64 decoded)
* No test coverage. unit tests and mock should be added.
* At least github action should be added to keep the PEP8 compliance.
* The source code is monofile. Should at least split the storage implementation in its own module

## Efficiency

The current implementation severely degrades data writing performance. The base64 encode should be
removed and data natively stored in the DB.

No concurrency implementation. This project does not use CPUs efficiently.

## Security

In order to limit the supply chain attack style, we can use the `requirements_sha256.txt` pip
configuration in order to setup the virtual env.
The `pip` command might should also use the following arguments:
`--no-deps     --only-binary
:all:`

However, due to the hardware limitation, the
provided one might not correspond to every running environement. Use it as an example.
A better way would be to create a dockerfile with this requirements and archive the hand build
container with all dependancies. One way to handle disaster recovery.


