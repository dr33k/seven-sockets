package com.seven.sockets.presence.serializable;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;

@Data
@NoArgsConstructor
public class PresenceStatus {
    private ZonedDateTime lastSeen;

    public PresenceStatus(ZonedDateTime lastSeen) {
        this.lastSeen = lastSeen;
    }
}
