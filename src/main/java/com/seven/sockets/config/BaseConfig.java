package com.seven.sockets.config;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafkaRetryTopic
@EnableKafka
public class BaseConfig {

    @Bean
    public ReactiveRedisTemplate<String, String> redisTmpl(ReactiveRedisConnectionFactory factory){
        RedisSerializer<String> srlzr = new StringRedisSerializer();
        RedisSerializationContext<String, String> ctxt = RedisSerializationContext
                .<String, String>newSerializationContext(srlzr)
                .value(srlzr)
                .hashKey(srlzr)
                .hashValue(srlzr)
                .build();
        return new ReactiveRedisTemplate<>(factory, ctxt);

    }
}
