# _SEVEN SOCKETS_

Reactive Chat application built with Spring Webflux/Project Reactor.

Uses GraphQL API for lighter exchange payloads. Kafka Event Queue for asynchronous writes and Redis Store for fast reads.

Functionalities to be provided:

* User presence and Last Seen records: **IN PROGRESS**
* User authentication with Seven ID: **TODO**
* Sending and receiving of private messages: **TODO**
* Sending and receiving of group messages: **TODO**
* Private calls: **TODO**
* Group calls: **TODO**


## Requirements

Java version: 21+

Maven version: 3.9.6+

Postgres version: 14+

Relational DB Name: acct_profile_db (Relational DB was used for user last seen records)

Env:

    PG_USER, PG_PASSWORD, PG_PORT, JWT_SECRET_KEY


## Run application

In project root folder
    
    $ mvn clean install
    $ docker-compose up
    $ mvn spring-boot:run


Visit http://localhost:8080/graphiql

