## SEVEN SOCKETS

Instant Messaging Application built with Go. 

Uses Seven ID for Authentication, Authorization and Auditing.

#### The Spring Webflux Implementation of this project was discontinued by me and this was chosen over it due to the several advantages the Golang runtime has against Webflux and the JVM considering High Concurrency, Lightweightness, Memory Consumption and Continuous Streaming. The Webflux project still exists as a learning project.



Uses REST API, Kafka Event Queue for asynchronous writes and Redis Store for fast reads.

Functionalities to be provided:

* User authentication with Seven ID: **IN PROGRESS**
* User presence and Last Seen records: **IN PROGRESS**
* Sending and receiving of private messages: TODO
* Sending and receiving of group messages: TODO
* Private calls: TODO
* Group calls: TODO


## Requirements

Go version: 1.27.1+

Postgres version: 14+

Relational DB Name: acct_profile_db (Relational DB was used for user last seen records)

Env:

    PG_USER, PG_PASSWORD, PG_PORT, JWT_SECRET_KEY


## Run application
Make sure a postgresql 14+ server is running.


In project root folder
    
    $ docker-compose up
    $ go run .


Visit http://localhost:8080
