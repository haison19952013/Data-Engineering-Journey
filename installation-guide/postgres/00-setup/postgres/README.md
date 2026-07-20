## 1. Create network

```shell
docker network create streaming-network --driver bridge
```

## 2. Run postgres

```shell
docker compose up -d
```

**Check Status & Logs**

```shell
docker compose ps
docker compose logs postgres -f -n 100
```

## 3. Monitor

Access the `adminer` address and enter the `postgres` connection details (see `environment` in `docker-compose.yml`).

[adminer](http://localhost:8380)

**Note:** `Adminer` is just a database connection tool; you can use other tools such as `pgAdmin`, `DBeaver`, etc.

## References

[Postgres Docker Image](https://hub.docker.com/_/postgres)