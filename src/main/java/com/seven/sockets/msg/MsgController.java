package com.seven.sockets.msg;

import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.graphql.data.method.annotation.SubscriptionMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import java.util.List;

@Controller
public class MsgController {

    @SubscriptionMapping
    public Flux<Msg> newMsgs(){
        return Flux.empty();
    }

    @MutationMapping
    public Msg sendMsg(@Argument String content, @Argument String to, @Argument List<String> objects){
        return null;
    }
}
