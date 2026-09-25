package com.seven.sockets.presence;

import com.seven.sockets.acct_profile.repos.LastSeenRepository;
import com.seven.sockets.presence.util.Constants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Slf4j
@RequiredArgsConstructor
public class PresenceStatusEventConsumer {
    private final LastSeenRepository lastSeenRepository;

    @KafkaListener(
            topics = Constants.PRESENCE_KFK_TOPIC,
            groupId = Constants.PRESENCE_KFK_GRP_ID,
            concurrency = "3"
    )
    @RetryableTopic
    public void persistPresenceStatus(
            @Payload PresenceStatus status,
            @Header(KafkaHeaders.RECEIVED_KEY) String acctId
    ){
        var acctIdUUID = UUID.fromString(acctId);
        log.debug("Persisting presence status  {} for acct: {}", status, acctId);

        lastSeenRepository.updateLastSeen(acctIdUUID, status.getLastSeen()).then()
                .doOnError(e -> log.error("Failed to persist presence status for acct: {}", acctId))
                .subscribe();
    }
}
