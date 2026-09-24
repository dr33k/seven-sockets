package com.seven.sockets.presence;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.ZonedDateTime;

@Data
@AllArgsConstructor
public class PresenceStatus {
    private ZonedDateTime lastSeen;
}
