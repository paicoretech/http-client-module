package com.client.http.http;

import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.utils.InterpreterUtils;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.client.http.utils.Constants;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.ws.SocketSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.client.http.utils.AppProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import com.paicbd.smsc.utils.RedisManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.client.http.utils.Constants.DLR_EVENT_TYPE;
import static com.client.http.utils.Constants.INPUT_DIRECTION;
import static com.client.http.utils.Constants.IS_STARTED;
import static com.client.http.utils.Constants.MESSAGE_EVENT_TYPE;

@Slf4j
@Component
@RequiredArgsConstructor
public class HttpClientManager {
    private final RedisManager redisManager;
    private final AppProperties appProperties;
    private final SocketSession socketSession;

    private final ConcurrentMap<String, GatewayHttpConnection> httpConnectionManagerList;
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap;
    private final ConcurrentMap<String, Gateway> gatewayConcurrentHashMap;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConsumerFactory kafkaConsumerFactory;

    @PostConstruct
    public void startManager() {
        loadHttpConnectionManager();
        loadErrorCodeMapping();
    }

    private void loadHttpConnectionManager() {
        try {
            var gatewaysMaps = this.redisManager.hgetAll(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME);
            if (gatewaysMaps.isEmpty()) {
                log.warn("No gateways found on loadHttpConnectionManager");
                return;
            }

            gatewaysMaps.values().forEach(gatewayInRaw -> {
                Gateway gateway = Converter.stringToObject(gatewayInRaw.replace("\\\\", "\\"), Gateway.class);
                Objects.requireNonNull(gateway);
                if ("HTTP".equalsIgnoreCase(gateway.getProtocol())) {
                    GatewayHttpConnection gatewayHttpConnection = new GatewayHttpConnection(
                            appProperties, redisManager, gateway,
                            errorCodeMappingConcurrentHashMap,
                            scyllaManager,
                            kafkaTemplate,
                            kafkaConsumerFactory);

                    httpConnectionManagerList.put(String.valueOf(gateway.getNetworkId()), gatewayHttpConnection);
                    gatewayConcurrentHashMap.put(gateway.getSystemId(), gateway);
                    this.setInterpreterByNetworkId(gateway, MESSAGE_EVENT_TYPE);
                    this.setInterpreterByNetworkId(gateway, DLR_EVENT_TYPE);
                    if (gateway.getEnabled() == IS_STARTED) {
                        gatewayHttpConnection.connect();
                    }
                }
            });
            log.info("{} gateways loaded successfully", gatewaysMaps.size());
        } catch (Exception e) {
            log.error("Error on loadHttpConnectionManager: {}", e.getMessage());
        }
    }

    public void updateGateway(String stringNetworkId) {
        if (stringNetworkId != null) {
            String gatewayInRaw = redisManager.hget(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, stringNetworkId);
            if (gatewayInRaw == null) {
                log.warn("No gateway found for networkId {} on updateGateway", stringNetworkId);
                return;
            }
            Gateway gateway = Converter.stringToObject(gatewayInRaw, Gateway.class);
            Objects.requireNonNull(gateway);
            if (!"HTTP".equalsIgnoreCase(gateway.getProtocol())) {
                log.warn("This gateway {} is not handled by this application. Failed to update", stringNetworkId);
                return;
            }

            if (httpConnectionManagerList.containsKey(stringNetworkId)) {
                GatewayHttpConnection gatewayHttpConnection = httpConnectionManagerList.get(stringNetworkId);
                gatewayHttpConnection.setGateway(gateway);
                gatewayHttpConnection.setInterpreters(gateway.getInterpreter());

            } else {
                GatewayHttpConnection gatewayHttpConnection = new GatewayHttpConnection(
                        appProperties, redisManager, gateway,
                        errorCodeMappingConcurrentHashMap,
                        scyllaManager,
                        kafkaTemplate,
                        kafkaConsumerFactory);
                httpConnectionManagerList.put(stringNetworkId, gatewayHttpConnection);
            }

            gatewayConcurrentHashMap.put(gateway.getSystemId(), gateway);
            this.setInterpreterByNetworkId(gateway, MESSAGE_EVENT_TYPE);
            this.setInterpreterByNetworkId(gateway, DLR_EVENT_TYPE);
        } else {
            log.warn("No gateways found for connect on method updateGateway");
        }
    }

    public void connectGateway(String stringNetworkId) {
        if (stringNetworkId != null) {
            GatewayHttpConnection gatewayHttpConnection = httpConnectionManagerList.get(stringNetworkId);
            if (Objects.isNull(gatewayHttpConnection)) { // Probably is an SMPP gateway trying to connect, this is not handled by this application
                log.warn("This gateway is not handled by this application, {}", stringNetworkId);
                return;
            }
            gatewayHttpConnection.connect();
            socketSession.sendStatus(stringNetworkId, Constants.PARAM_UPDATE_STATUS, "STARTED");
        } else {
            log.warn("No gateways found for connect on method connectGateway");
        }
    }

    public void stopGateway(String stringNetworkId) {
        if (stringNetworkId != null) {
            log.info("Stopping gateway with networkId {}", stringNetworkId);
            GatewayHttpConnection gatewayHttpConnection = httpConnectionManagerList.get(stringNetworkId);
            if (Objects.isNull(gatewayHttpConnection)) {
                log.warn("The gateway with networkId {} is not handled by this application", stringNetworkId);
                return;
            }
            gatewayHttpConnection.stopConnection();
            gatewayConcurrentHashMap.put(gatewayHttpConnection.getGateway().getSystemId(), gatewayHttpConnection.getGateway());
            socketSession.sendStatus(stringNetworkId, Constants.PARAM_UPDATE_STATUS, Constants.STOPPED);
        } else {
            log.warn("No gateways found for stop on method stopGateway");
        }
    }

    private void loadErrorCodeMapping() {
        try {
            var errorCodeMappingMap = redisManager.hgetAll(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME);
            if (errorCodeMappingMap.isEmpty()) {
                log.warn("No Error code mapping found on loadErrorCodeMapping");
                return;
            }

            errorCodeMappingMap.forEach((key, errorCodeMappingInRaw) -> {
                List<ErrorCodeMapping> errorCodeMappingList = Converter.stringToObject(errorCodeMappingInRaw, new TypeReference<>() {
                });
                errorCodeMappingConcurrentHashMap.put(key, errorCodeMappingList);
            });
            log.info("{} error code mapping loaded successfully", errorCodeMappingMap.size());
        } catch (Exception e) {
            log.error("Error on loadErrorCodeMapping: {}", e.getMessage());
        }
    }

    public void updateErrorCodeMapping(String mnoId) {
        if (mnoId == null || mnoId.isEmpty()) {
            log.warn("No Error code mapping found for mnoId null or empty");
            return;
        }

        String errorCodeMappingInRaw = redisManager.hget(GeneralSmscConstants.ERROR_CODE_MAPPING_HASH_NAME, mnoId);
        if (errorCodeMappingInRaw == null) {
            errorCodeMappingConcurrentHashMap.remove(mnoId); // Remove if existed, if not exist do anything
            return;
        }
        List<ErrorCodeMapping> errorCodeMappingList = Converter.stringToObject(errorCodeMappingInRaw, new TypeReference<>() {
        });
        errorCodeMappingConcurrentHashMap.put(mnoId, errorCodeMappingList); // Put do it the replacement if existed
    }

    public void deleteGateway(String stringNetworkId) {
        log.warn("Deleting gateway {}", stringNetworkId);
        GatewayHttpConnection gatewayHttpConnection = httpConnectionManagerList.remove(stringNetworkId);
        if (Objects.isNull(gatewayHttpConnection)) {
            log.warn("The gateway with networkId {} is not handled by this application. Failed to delete", stringNetworkId);
            return;
        }

        gatewayConcurrentHashMap.remove(gatewayHttpConnection.getGateway().getSystemId());
        gatewayHttpConnection.stopConnection();
        this.removeInterpreterByNetworkId(gatewayHttpConnection.getGateway().getSystemId());
        redisManager.hdel(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, stringNetworkId);
    }

    private void setInterpreterByNetworkId(Gateway gateway, String eventType) {
        PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), eventType, INPUT_DIRECTION);
        if (Objects.nonNull(payloadMapper) && !payloadMapper.isDefaultTemplate()) {
            String key = InterpreterUtils.createKeyToMapForPath(eventType, payloadMapper.getPath());
            InterpreterByPathRequest interpreterByPathRequest = new InterpreterByPathRequest();
            interpreterByPathRequest.setSystemId(gateway.getSystemId());
            interpreterByPathRequest.setPayloadMappers(payloadMapper);
            interpreterByPathRequestConcurrentHashMap.put(key, interpreterByPathRequest);
        }
    }

    private void removeInterpreterByNetworkId(String systemId) {
        List<String> keysToRemove = new ArrayList<>();
        interpreterByPathRequestConcurrentHashMap.forEach((key, interpreter) -> {
            if (interpreter.getSystemId().equals(systemId)) {
                keysToRemove.add(key);
            }
        });

        keysToRemove.forEach(interpreterByPathRequestConcurrentHashMap::remove);
    }
}
