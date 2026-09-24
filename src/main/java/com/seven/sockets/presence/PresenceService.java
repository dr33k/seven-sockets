package com.seven.sockets.presence;

import com.seven.sockets.util.Constants;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
public class PresenceService {

    private final ReactiveRedisTemplate<String, String> redisTmpl;
    private final KafkaTemplate<String, PresenceStatus> kafkaTmpl;

    public PresenceService(ReactiveRedisTemplate<String, String> redisTmpl, KafkaTemplate<String, PresenceStatus> kafkaTmpl) {
        this.redisTmpl = redisTmpl;
        this.kafkaTmpl = kafkaTmpl;
    }

    public Mono<Boolean> updateLastSeen(UUID userId){
        return redisTmpl.opsForZSet().add(Constants.PRESENCE_RDS_KEY, userId.toString(), Instant.now().getEpochSecond());
    }

}
