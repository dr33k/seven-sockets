package com.seven.sockets.presence;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class PresenceStatus {
    private ZonedDateTime lastSeen;

    public PresenceStatus(ZonedDateTime lastSeen) {
        this.lastSeen = lastSeen;
    }
}
