package com.seven.sockets.presence;

import com.seven.sockets.presence.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PresenceEventConsumer {

    @KafkaListener(
            topics = Constants.PRESENCE_KFK_TOPIC,
            groupId = Constants.PRESENCE_KFK_GRP_ID,
            concurrency = "3"
    )
    @RetryableTopic
    public void processPresenceStatus(
            @Payload PresenceStatus status,
            @Header(KafkaHeaders.RECEIVED_KEY) String acctId
    ){

    }
}
