package com.seven.sockets.acct;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.UUID;

import static com.seven.sockets.util.Constants.PRESENCE_RDS_KEY;
import static com.seven.sockets.util.Constants.PRESENCE_KFK_TOPIC;

@Service
public class PresenceService {

    private final ReactiveRedisTemplate<String, String> redisTmpl;
    private final KafkaTemplate<String, PresenceStatus> kafkaTmpl;

    public PresenceService(ReactiveRedisTemplate<String, String> redisTmpl, KafkaTemplate<String, PresenceStatus> kafkaTmpl) {
        this.redisTmpl = redisTmpl;
        this.kafkaTmpl = kafkaTmpl;
    }

    public Mono<Boolean> updateLastSeen(String acctId) {
        var now = Instant.now();
        return redisTmpl.opsForZSet().score(PRESENCE_RDS_KEY, acctId)
            .flatMap(lastHeartbeat -> {
                if (now.getEpochSecond() - lastHeartbeat > 60) {//If coming online after a while
                    return sendPresenceEvent(acctId, Boolean.TRUE);
                }
                return Mono.empty();
            })
            .switchIfEmpty(sendPresenceEvent(acctId, Boolean.TRUE))
            .then(redisTmpl.opsForZSet().add(PRESENCE_RDS_KEY, acctId, now.getEpochSecond()));
    }

    private Mono<Void> sendPresenceEvent(String acctId, Boolean isOnline){
        return Mono.fromFuture(kafkaTmpl.send(PRESENCE_KFK_TOPIC, acctId, new PresenceStatus(isOnline, ZonedDateTime.now())))
                .then();
    }
    public Flux<PresenceStatus> streamPresence(UUID acctId) {
        return Flux.empty();
    }
}
