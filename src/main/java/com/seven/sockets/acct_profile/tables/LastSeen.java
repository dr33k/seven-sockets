package com.seven.sockets.acct_profile.tables;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.ZonedDateTime;
import java.util.UUID;

@Data
@Table
@NoArgsConstructor
public class LastSeen {
    @Id
    @Column("acct_id")
    private UUID acctId;

    @Column("timestamp")
    private ZonedDateTime timestamp;
}
