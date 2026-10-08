package com.seven.sockets.presence;

import com.seven.auth.account.AccountDTO;
import com.seven.auth.client.authorization.Authorize;
import com.seven.sockets.presence.serializable.PresenceStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Controller
@Slf4j
public class PresenceController {
    private final PresenceService presenceSvc;

    public PresenceController(PresenceService presenceSvc) {
        this.presenceSvc = presenceSvc;
    }

    @MutationMapping
    @Authorize
    public Mono<Void> heartbeat(@AuthenticationPrincipal AccountDTO.Record principal) {
        log.debug("Heartbeat for Account {}", principal);
        return presenceSvc.updateLastSeen(principal.id().toString()).then();
    }

    @SubscriptionMapping
    public Flux<PresenceStatus> presenceStatusStream(UUID acctId){
        return null;
    }
}
