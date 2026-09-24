package com.seven.sockets.presence;

import com.seven.auth.dto.account.IAccount;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Controller
public class PresenceController {
    private final PresenceService presenceSvc;

    public PresenceController(PresenceService presenceSvc) {
        this.presenceSvc = presenceSvc;
    }

    @MutationMapping
    public Mono<Void> heartbeat(@AuthenticationPrincipal IAccount.Record principal) {
        return presenceSvc.updateLastSeen(principal.id());
    }

    @SubscriptionMapping
    public Flux<PresenceStatus> presenceStatusStream(UUID acctId){
        return null;
    }
}
