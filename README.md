<p align="center">
  <img src="yeeKV_logo_512.png" alt="yeeKV Logo" width="200"/>
</p>


# yeeKV

**yeeKV** is a lightweight Redis-like key-value store written in Go.


## Features

* **RESP Protocol** : Compatible with Redis clients
* **Multiple Data Structures** : Strings, Lists, Streams
* **Key Expiration** : Millisecond precision (`PX`)



## Installation & Usage

### 1. Start the yeeKV Server

```bash
go build -o yeekv main.go
./yeekv
```

The server listens on `127.0.0.1:6379` by default.


### 2. Use the built-in CLI

```bash
go build -o yeekv-cli cli.go
./yeekv-cli
```

Example session:

```
yeeKV CLI connected. Type commands (type 'exit' to quit).
yeeKV> SET foo bar
OK
yeeKV> GET foo
bar
yeeKV> exit
bye~
```

## Supported Commands & Examples

### String

| Command                         | Example               |
| ------------------------------- | --------------------- |
| `SET key value`                 | `SET foo bar`         |
| `SET key value PX milliseconds` | `SET foo bar PX 5000` |
| `GET key`                       | `GET foo`             |


### List

| Command                       | Example              |
| ----------------------------- | -------------------- |
| `RPUSH key value [value ...]` | `RPUSH mylist a b c` |
| `LPUSH key value [value ...]` | `LPUSH mylist x y`   |
| `LPOP key [count]`            | `LPOP mylist 2`      |
| `BLPOP key timeout`           | `BLPOP mylist 5`     |
| `LRANGE key start stop`       | `LRANGE mylist 0 -1` |
| `LLEN key`                    | `LLEN mylist`        |

### Type

| Command    | Example       |
| ---------- | ------------- |
| `TYPE key` | `TYPE mylist` |


### Stream

| Command                                             | Example                          |
| --------------------------------------------------- | -------------------------------- |
| `XADD stream_name id field value [field value ...]` | `XADD mystream * temperature 96` |
| `XRANGE stream_name start end`                      | `XRANGE mystream - +`            |
| `XREAD STREAMS stream_name id`                      | `XREAD STREAMS mystream 0`       |


## Command Output Example

```
yeeKV> RPUSH mylist a b c
3
yeeKV> LRANGE mylist 0 -1
Array of 3 elements:
1) a
2) b
3) c
```


## Roadmap

* More Redis commands (DEL, EXPIRE, INCR, etc.)
* Optimized storage & memory management
* Persistence (RDB / AOF)
* Cluster mode
* Pub/Sub support

## License

This project is licensed under the MIT License.

It includes third-party software licensed by CodeCrafters under the MIT License.