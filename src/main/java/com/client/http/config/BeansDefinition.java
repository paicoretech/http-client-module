package com.client.http.config;

import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.http.GatewayHttpConnection;
import com.client.http.utils.AppProperties;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.ws.SocketSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;
import com.paicbd.smsc.utils.RedisManager;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Generated
@Configuration
@RequiredArgsConstructor
public class BeansDefinition {
    private final AppProperties appProperties;

    @Bean
    public RedisManager redisManager() {
        return Converter.paramsToRedisManager(
                new UtilsRecords.JedisConfigParams(appProperties.getRedisNodes(), appProperties.getRedisMaxTotal(),
                        appProperties.getRedisMaxIdle(), appProperties.getRedisMinIdle(),
                        appProperties.isRedisBlockWhenExhausted(), appProperties.getRedisConnectionTimeout(),
                        appProperties.getRedisSoTimeout(), appProperties.getRedisMaxAttempts(),
                        appProperties.getRedisUser(), appProperties.getRedisPassword(),
                        appProperties.isRedisStandaloneEnabled())
        );
    }

    @Bean
    public SocketSession socketSession() {
        return new SocketSession("gw"); // Gateways
    }

    @Bean
    public ConcurrentMap<String, GatewayHttpConnection> httpConnectionManagerList() {
        return new ConcurrentHashMap<>(); // stringNetworkId -> GatewayHttpConnection
    }

    @Bean
    public ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<String, Gateway> gatewayConcurrentHashMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ScyllaManager scyllaManager() {
        return new ScyllaManager(
                appProperties.getContactPoints(),
                appProperties.getLocalDatacenter(),
                appProperties.getUsername(),
                appProperties.getPassword(),
                false
        );
    }
}
