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

Truy cập vào địa chỉ của `adminer` và nhập caác thông tin của `postgres` (Xem `environment` trong `docker-compose.yml`).

[adminer](http://localhost:8380)

**Lưu ý:** `Adminer` chỉ là công cụ kết nối đến db, bạn có thể dùng công cụ khác thay thế như `pgAdmin`, `DBeaver`, ...

## References

[Postgres Docker Image](https://hub.docker.com/_/postgres)