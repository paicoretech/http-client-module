package com.client.http.service;

import com.client.http.components.ProxyResponseHandler;
import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.exception.NoInterpreterFoundException;
import com.client.http.exception.SmsDeliveryException;
import com.client.http.exception.SmsProcessingException;
import com.client.http.utils.AppProperties;
import com.client.http.utils.InterpreterUtils;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.interpreter.PayloadInterpreter;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.client.http.utils.Constants.INPUT_DIRECTION;
import static com.client.http.utils.Constants.MESSAGE_EVENT_TYPE;
import static com.client.http.utils.Constants.OUTPUT_DIRECTION;
import static com.client.http.utils.Constants.RESPONSE_MESSAGE_EVENT_TYPE;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessMessageService {
    private final AppProperties appProperties;
    private final ConcurrentMap<String, Gateway> gatewayConcurrentHashMap;
    private final ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProxyResponseHandler proxyResponseHandler;
    private MediaType mediaType;

    public Map<String, Object> processMessage(Map<String, Object> bodyRequest, String pathRequest, MediaType contentType) {
        PayloadMapper payloadMapper = null;
        String systemId = this.getSystemId(bodyRequest);
        this.mediaType = contentType;

        if (systemId.isBlank()) {
            String key = InterpreterUtils.createKeyToMapForPath(MESSAGE_EVENT_TYPE, pathRequest);
            InterpreterByPathRequest interpreter = interpreterByPathRequestConcurrentHashMap.getOrDefault(key, null);
            if (Objects.isNull(interpreter)) {
                throw new NoInterpreterFoundException(String.format("No interpreter found for message MO request in the path %s", pathRequest), this.mediaType);
            }
            payloadMapper = interpreter.getPayloadMappers();
            systemId = interpreter.getSystemId();
        }

        Gateway gateway = gatewayConcurrentHashMap.get(systemId);
        if (InterpreterUtils.gatewayIsInvalid(gateway)) {
            throw new SmsProcessingException("Gateway was not found for request message MO", this.mediaType);
        }

        if (Objects.nonNull(gateway) && Objects.isNull(payloadMapper)) {
            payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), MESSAGE_EVENT_TYPE, INPUT_DIRECTION);
        }

        return this.processMessageMO(bodyRequest, pathRequest, payloadMapper, gateway);
    }

    private Map<String, Object> processMessageMO(Map<String, Object> bodyRequest, String pathRequest, PayloadMapper interpreter, Gateway gateway) {
        if (Objects.nonNull(interpreter)) {
            MessageEvent messageEvent = new MessageEvent();
            InterpreterUtils.interpreterFormatForMO(messageEvent, bodyRequest, interpreter);
            this.prepareMessage(messageEvent, gateway);
            PayloadInterpreter.convertShortMessageByEncodingType(messageEvent, gateway);
            messageEvent.setUseProxy(interpreter.isUseProxy());
            messageEvent.setSmscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY);
            if (interpreter.isUseProxy()) {
                // Register before sending downstream so a fast confirmation cannot arrive before we are waiting.
                proxyResponseHandler.register(messageEvent.getMessageId());
            }
            kafkaTemplate.send(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC, messageEvent.toString());
            messageEvent.setShortMessage("message process successfully");

            var response = this.processResponseMessageMO(messageEvent, gateway, gateway.getSystemId());
            if (interpreter.isUseProxy()) {
                UtilsRecords.HttpProxyResponse proxyResponse = proxyResponseHandler.waitForResponse(
                        messageEvent.getMessageId(), appProperties.getProxyModeResponseTimeout());
                if (Objects.nonNull(proxyResponse)) {
                    if (proxyResponse.error()) {
                        throw new SmsDeliveryException(proxyResponse.toString(),
                                MediaType.valueOf("application/" + interpreter.getBodyType().toUpperCase()));
                    }
                    return response;
                } else {
                    String bodyType = interpreter.getBodyType().toUpperCase();
                    throw new SmsDeliveryException("Error waiting for proxy response in destination system",
                            MediaType.valueOf("application/" + bodyType));
                }
            }

            return response;
        }

        log.warn("No interpreter found for message MO {}", pathRequest);
        throw new NoInterpreterFoundException(String.format("No interpreter found for gateway %s", gateway.getSystemId()), this.mediaType);
    }

    private void prepareMessage(MessageEvent messageEvent, Gateway gateway) {
        var messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        messageEvent.setId(messageId);
        messageEvent.setOriginNetworkId(gateway.getNetworkId());
        messageEvent.setOriginProtocol("HTTP");
        messageEvent.setOriginNetworkType("GW");
        messageEvent.setSystemId(gateway.getSystemId());
        messageEvent.setMessageId(messageId);
        messageEvent.setParentId(messageId);
    }

    private Map<String, Object> processResponseMessageMO(MessageEvent messageEvent, Gateway gateway, String systemId) {
        PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), RESPONSE_MESSAGE_EVENT_TYPE, OUTPUT_DIRECTION);

        if (Objects.nonNull(payloadMapper)) {
            String bodyResponse = InterpreterUtils.interpreterFormatForMT(messageEvent, payloadMapper);
            Map<String, Object> response = InterpreterUtils.createResponseByBodyType(bodyResponse, payloadMapper.getBodyType());

            Map<String, String> headers = new HashMap<>();
            InterpreterUtils.forEachInterpreterHeader(
                    gateway, messageEvent, OUTPUT_DIRECTION, RESPONSE_MESSAGE_EVENT_TYPE,
                    headers::put
            );

            if (!headers.isEmpty()) {
                response.put("headers", headers);
            }
            return response;
        }

        log.error("No interpreter found for message MO, system id {}", systemId);
        throw new NoInterpreterFoundException("No interpreter found for response message MO", this.mediaType);
    }

    private String getSystemId(Map<String, Object> bodyRequest) {
        Object systemId = bodyRequest.get("system_id");
        if (Objects.isNull(systemId)) {
            systemId = bodyRequest.get("systemId");
        }
        return Objects.nonNull(systemId) ? systemId.toString() : "";
    }
}
