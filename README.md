# _SEVEN SOCKETS_

Reactive Chat application built with Spring Webflux/Project Reactor.

Uses Seven ID modules for Authentication, Authorization and Accounting/Audit.

Uses GraphQL API for lighter exchange payloads. Kafka Event Queue for asynchronous writes and Redis Store for fast reads.

Functionalities to be provided:

* User authentication with Seven ID: **DONE**
* User presence and Last Seen records: **IN PROGRESS**
* Sending and receiving of private messages: TODO
* Sending and receiving of group messages: TODO
* Private calls: TODO
* Group calls: TODO


## Requirements

Java version: 25+

Maven version: 3.9.6+

Postgres version: 14+

Relational DB Name: acct_profile_db (Relational DB was used for user last seen records)

Env:

    PG_USER, PG_PASSWORD, PG_PORT, JWT_SECRET_KEY


## Run application
Make sure a postgresql 14+ server is running.


In project root folder
    
    $ mvn clean install
    $ docker-compose up
    $ mvn spring-boot:run


Visit http://localhost:8080/graphiql

