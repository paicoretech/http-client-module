package com.client.http.utils;

import lombok.Getter;
import com.paicbd.smsc.utils.Generated;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;

@Getter
@Generated
@Component
public class AppProperties {
    // Redis
    @Value("#{'${redis.cluster.nodes}'.split(',')}")
    private List<String> redisNodes;

    @Value("${redis.threadPool.maxTotal:20}")
    private int redisMaxTotal;

    @Value("${redis.threadPool.maxIdle:20}")
    private int redisMaxIdle;

    @Value("${redis.threadPool.minIdle:1}")
    private int redisMinIdle;

    @Value("${redis.threadPool.blockWhenExhausted:true}")
    private boolean redisBlockWhenExhausted;

    @Value("${redis.connection.timeout:0}")
    private int redisConnectionTimeout;

    @Value("${redis.so.timeout:0}")
    private int redisSoTimeout;

    @Value("${redis.maxAttempts:0}")
    private int redisMaxAttempts;

    @Value("${redis.connection.password:}")
    private String redisPassword;

    @Value("${proxy.mode.responseTimeout:5000}")
    private long proxyModeResponseTimeout;

    @Value("${redis.connection.user:}")
    private String redisUser;

    @Value("${redis.standalone.enabled:false}")
    private boolean redisStandaloneEnabled;

    // WebSocket
    @Value("${websocket.server.host:localhost}")
    private String webSocketHost;

    @Value("${websocket.server.port:9000}")
    private int webSocketPort;

    @Value("${websocket.server.path:/ws}")
    private String webSocketPath;

    @Value("${websocket.server.enabled:false}")
    private boolean websocketEnabled;

    @Value("${websocket.header.name:Authorization}")
    private String websocketHeaderName;

    @Value("${websocket.header.value}")
    private String websocketHeaderValue;

    @Value("${websocket.retry.intervalSeconds}")
    private int websocketRetryInterval;

    @Value("${application.useHttp2}")
    private boolean http2;

    // Workers per gw
    @Value("${scylla.contact.points}")
    private String contactPoints;

    @Value("${scylla.datacenter}")
    private String localDatacenter;

    @Value("${scylla.user}")
    private String username;

    @Value("${scylla.password}")
    private String password;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.listener.concurrency}")
    private int kafkaListenerConcurrency;

    @Value("${smsc.default.dlr.data-coding:0}")
    private int smscDefaultDlrDataCoding;

    @Value("${spring.kafka.consumer.reconnect.backoff.ms:1000}")
    private long kafkaConsumerReconnectBackoffMs;

    @Value("${spring.kafka.consumer.reconnect.backoff.max.ms:10000}")
    private long kafkaConsumerReconnectBackoffMaxMs;

    @Value("${spring.kafka.consumer.session.timeout.ms:30000}")
    private int kafkaConsumerSessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat.interval.ms:10000}")
    private int kafkaConsumerHeartbeatIntervalMs;

    @Value("${spring.kafka.producer.reconnect.backoff.ms:1000}")
    private long kafkaProducerReconnectBackoffMs;

    @Value("${spring.kafka.producer.reconnect.backoff.max.ms:10000}")
    private long kafkaProducerReconnectBackoffMaxMs;
}
