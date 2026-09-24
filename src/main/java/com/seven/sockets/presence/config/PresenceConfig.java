package com.seven.sockets.presence.config;

import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafkaRetryTopic
@EnableKafka
public class PresenceConfig {

    @Bean
    public ReactiveRedisTemplate<String, String> redisTmpl(ReactiveRedisConnectionFactory factory){
        RedisSerializer<String> stringRedisSerializer = new StringRedisSerializer();
        RedisSerializationContext<String, String> ctxt = RedisSerializationContext
                .<String, String>newSerializationContext(stringRedisSerializer)
                .value(stringRedisSerializer)
                .hashKey(stringRedisSerializer)
                .hashValue(stringRedisSerializer)
                .build();
        return new ReactiveRedisTemplate<>(factory, ctxt);

    }
}
