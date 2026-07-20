## 1. Run netcat service

```shell
docker container stop netcat || true &&
docker container rm netcat || true &&
docker run -ti --name netcat \
--network=streaming-network \
alpine:3.14 \
/bin/sh -c "apk add --no-cache netcat-openbsd && nc -lk 9999"
```

## 2. Run the program

```shell
docker container stop structured-streaming || true &&
docker container rm structured-streaming || true &&
docker run -ti --name structured-streaming \
--network=streaming-network \
-p 4040:4040 \
-v ./:/spark \
unigap/spark:3.5 spark-submit \
/spark/10-structured-streaming/structured_streaming.py
```

## 3. Exercises

### 3.1 Exercise 1

Write a program to count words and print a list of words that appear an even number of times.

Expected result:

| word | count |
|------|-------|
| x    | 2     |
| y    | 4     |

### 3.2 Exercise 2

Write a program to count words and print a list of words with a length greater than 1 that appear an odd number of times.

Expected result:

| word | count |
|------|-------|
| yy   | 1     |
| zz   | 3     |
| ttt  | 1     |
