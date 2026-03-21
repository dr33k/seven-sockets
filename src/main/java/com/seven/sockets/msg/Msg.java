package com.seven.sockets.msg;

import java.time.ZonedDateTime;
import java.util.List;

public class Msg {
    private String id;
    private String content;
    private List<String> objects;
    private String from;
    private String to;
    private ZonedDateTime sentTms;
    private ZonedDateTime deliveryTms;
    private ZonedDateTime receiptTms;
}
