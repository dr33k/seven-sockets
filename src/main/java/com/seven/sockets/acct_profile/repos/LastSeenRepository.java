package com.seven.sockets.acct_profile.repos;

import com.seven.sockets.acct_profile.tables.LastSeen;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.ZonedDateTime;
import java.util.UUID;

@Repository
public interface LastSeenRepository extends R2dbcRepository<LastSeen, UUID> {

    @Query("UPDATE last_seen SET timestamp = :timestamp WHERE acct_id = :acctId")
    Mono<Void> updateLastSeen(UUID acctId, ZonedDateTime timestamp);
}
