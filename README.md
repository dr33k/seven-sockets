## SEVEN SOCKETS

Instant Messaging Application built with Go. 

Uses Seven ID for Authentication, Authorization and Auditing.

#### The Spring Webflux Implementation of this project was discontinued by me and this was chosen over it due to the several advantages the Golang runtime has against Webflux and the JVM considering High Concurrency, Lightweightness, Memory Consumption and Continuous Streaming. The Webflux project still exists as a learning project.



Uses REST API, Kafka Event Queue for asynchronous writes and Redis Store for fast reads.

Functionalities to be provided:

* User authentication with Seven ID: **DONE**
* User presence and Last Seen records: **IN PROGRESS**
* Sending and receiving of private messages: TODO
* Sending and receiving of group messages: TODO
* Private calls: TODO
* Group calls: TODO


## Requirements

Go version: 1.27.1+

Postgres version: 14+

Relational DB Name: acct_profile_db (Relational DB was used for user last seen records)

Env (If you should like to change them. Default values already provided):

    SOCKETS_SERVER_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_HOST, POSTGRES_DB_NAME, REDIS_PASSWORD, REDIS_ADDR, KAFKA_BROKER_ADDR, JWT_SECRET_KEY


## Run application

In project root folder
    
    $ docker-compose up -d
    $ go build
    $ ./sockets

Server runs on localhost port 8080