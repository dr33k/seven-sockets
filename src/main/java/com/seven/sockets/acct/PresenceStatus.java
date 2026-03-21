package com.seven.sockets.acct;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
public class PresenceStatus {
    private Boolean isOnline;
    private ZonedDateTime lastSeen;

    public PresenceStatus(Boolean isOnline, ZonedDateTime lastSeen) {
        this.isOnline = isOnline;
        this.lastSeen = lastSeen;
    }
}
