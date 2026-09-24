package com.seven.sockets.presence;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZonedDateTime;

import static com.seven.sockets.presence.util.Constants.*;

@Service
public class PresenceService {

    private final ReactiveRedisTemplate<String, String> redisTmpl;
    private final KafkaTemplate<String, PresenceStatus> kafkaTmpl;

    public PresenceService(ReactiveRedisTemplate<String, String> redisTmpl, KafkaTemplate<String, PresenceStatus> kafkaTmpl) {
        this.redisTmpl = redisTmpl;
        this.kafkaTmpl = kafkaTmpl;
    }

    /*
    Updates redis cache using the key(PRESENCE_RDS_KEY) : value(acctId) : score(timestamp in seconds) whenever invoked
    If a user's presence status was updated <= HEARTBEAT_INTERVAL_SECS + NETWORK LATENCY_SECS ago, do nothing
    Otherwise, send a presence event to kafka to persist presence status in the database
    If the user's record does not exist in the cache, insert it
     */
    public Mono<Boolean> updateLastSeen(String acctId){
        final long now = Instant.now().getEpochSecond();
        final long NETWORK_LATENCY_SECS = 3;

        return redisTmpl.opsForZSet().score(PRESENCE_RDS_KEY, acctId)
                .flatMap(lastHeartbeat ->{
                    if(now - lastHeartbeat > HEARTBEAT_INTERVAL_SECS + NETWORK_LATENCY_SECS){
                        return sendPresenceEvent(acctId);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(sendPresenceEvent(acctId))
                .then(redisTmpl.opsForZSet().add(PRESENCE_RDS_KEY, acctId, Instant.now().getEpochSecond()));
    }

    private Mono<Void> sendPresenceEvent(String acctId){
        var presenceStatus = new PresenceStatus(ZonedDateTime.now());
        return Mono.fromFuture(kafkaTmpl.send(PRESENCE_KFK_TOPIC, acctId, presenceStatus)).then();
    }

}
