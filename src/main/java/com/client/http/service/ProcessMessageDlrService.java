package com.client.http.service;

import com.client.http.components.ProxyResponseHandler;
import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.exception.NoInterpreterFoundException;
import com.client.http.exception.SmsDeliveryException;
import com.client.http.exception.SmsProcessingException;
import com.client.http.http.GatewayHttpConnection;
import com.client.http.utils.AppProperties;
import com.client.http.utils.Constants;
import com.client.http.utils.InterpreterUtils;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.interpreter.PayloadInterpreter;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.paicbd.smsc.utils.Converter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static com.client.http.utils.Constants.DLR_EVENT_TYPE;
import static com.client.http.utils.Constants.INPUT_DIRECTION;
import static com.client.http.utils.Constants.OUTPUT_DIRECTION;
import static com.client.http.utils.Constants.PROXY_MODE_CUSTOM_PARAM;
import static com.client.http.utils.Constants.RESPONSE_DLR_EVENT_TYPE;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessMessageDlrService {
    private final ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap;
    private final ConcurrentMap<String, Gateway> gatewayConcurrentHashMap;
    private final ConcurrentMap<String, GatewayHttpConnection> httpConnectionManagerList;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ProxyResponseHandler proxyResponseHandler;
    private final AppProperties appProperties;
    private MediaType mediaType;

    public Map<String, Object> processDlr(Map<String, Object> bodyRequest, String pathRequest, MediaType contentType) {
        String messageId = this.getMessageId(bodyRequest);
        this.mediaType = contentType;
        if (messageId.isBlank()) {
            return this.processDlrWithPathRequest(bodyRequest, pathRequest);
        }
        return this.processDlrWithDefaultTemplate(bodyRequest, messageId);
    }

    private Map<String, Object> processDlrWithDefaultTemplate(Map<String, Object> bodyRequest, String messageId) {
        String hashKey = Objects.nonNull(messageId) ? messageId.toUpperCase() : null;
        String existsSubmitResp = scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, hashKey);
        if (Objects.isNull(existsSubmitResp)) {
            throw new SmsProcessingException(String.format("Message id %s was not found", messageId), mediaType);
        }

        UtilsRecords.SubmitSmResponseEvent submitResult = Converter.stringToObject(existsSubmitResp, UtilsRecords.SubmitSmResponseEvent.class);
        if (Objects.isNull(submitResult)) {
            throw new SmsProcessingException(String.format("Invalid submit result cached for message id %s", messageId), mediaType);
        }
        GatewayHttpConnection gatewayHttpConnection = httpConnectionManagerList.getOrDefault(String.valueOf(submitResult.destNetworkId()), null);

        if (Objects.nonNull(gatewayHttpConnection)) {
            Gateway gateway = gatewayHttpConnection.getGateway();
            MessageEvent dlrEvent = new MessageEvent();
            PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), DLR_EVENT_TYPE, INPUT_DIRECTION);
            InterpreterUtils.interpreterFormatForMO(dlrEvent, bodyRequest, payloadMapper);
            PayloadInterpreter.convertShortMessageByEncodingType(dlrEvent, gateway);
            dlrEvent.setOriginNetworkId(gateway.getNetworkId());
            dlrEvent.setOriginProtocol(gateway.getProtocol());
            dlrEvent.setOriginNetworkType(Constants.ORIGIN_GATEWAY_TYPE);
            dlrEvent.setCheckSubmitSmResponse(true);
            this.publishDlrEvent(dlrEvent, submitResult);
            return this.processResponseDlrMO(dlrEvent, gateway.getSystemId(), gateway);
        }

        log.error("Process dlr error, Gateway was not found for system id <{}>", submitResult.systemId());
        throw new SmsProcessingException(
                String.format("Gateway was not found to process message id %s", messageId),
                mediaType);
    }

    private Map<String, Object> processDlrWithPathRequest(Map<String, Object> bodyRequest, String pathRequest) {
        String key = InterpreterUtils.createKeyToMapForPath(DLR_EVENT_TYPE, pathRequest);
        InterpreterByPathRequest interpreter = interpreterByPathRequestConcurrentHashMap.getOrDefault(key, null);
        if (Objects.nonNull(interpreter)) {
            MessageEvent dlrEvent = new MessageEvent();
            InterpreterUtils.interpreterFormatForMO(dlrEvent, bodyRequest, interpreter.getPayloadMappers());
            String hashKey = Objects.nonNull(dlrEvent.getMessageId()) ? dlrEvent.getMessageId().toUpperCase() : null;
            String existsSubmitResp = scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, hashKey);
            if (Objects.nonNull(existsSubmitResp)) {
                Gateway gateway = gatewayConcurrentHashMap.getOrDefault(interpreter.getSystemId(), null);
                if (Objects.isNull(gateway)) {
                    throw new SmsProcessingException(
                            String.format("Gateway was not found to process message id %s", dlrEvent.getMessageId()),
                            mediaType);
                }
                PayloadInterpreter.convertShortMessageByEncodingType(dlrEvent, gateway);
                dlrEvent.setCheckSubmitSmResponse(true);
                UtilsRecords.SubmitSmResponseEvent submitResult = Converter.stringToObject(existsSubmitResp, UtilsRecords.SubmitSmResponseEvent.class);
                this.publishDlrEvent(dlrEvent, submitResult);
                return this.processResponseDlrMO(dlrEvent, interpreter.getSystemId(), gateway);
            }
        }
        return Collections.emptyMap();
    }

    private void publishDlrEvent(MessageEvent dlrEvent, UtilsRecords.SubmitSmResponseEvent submitResult) {
        if (!isProxySubmit(submitResult)) {
            kafkaTemplate.send(KafkaTopicsConstants.PRE_DELIVER_TOPIC, dlrEvent.toString());
            return;
        }

        String correlationKey = submitResult.submitSmServerId();
        dlrEvent.setUseProxy(true);
        // Register before sending downstream so a fast confirmation cannot arrive before we are waiting.
        proxyResponseHandler.register(correlationKey);
        kafkaTemplate.send(KafkaTopicsConstants.PRE_DELIVER_TOPIC, dlrEvent.toString());

        UtilsRecords.HttpProxyResponse proxyResponse =
                proxyResponseHandler.waitForResponse(correlationKey, appProperties.getProxyModeResponseTimeout());

        if (Objects.isNull(proxyResponse)) {
            log.warn("Proxy mode: no deliver_sm confirmation for DLR with message id {} within {} ms",
                    correlationKey, appProperties.getProxyModeResponseTimeout());
            throw new SmsDeliveryException("Timeout waiting for deliver_sm confirmation in destination system", mediaType);
        }

        if (proxyResponse.error()) {
            log.warn("Proxy mode: deliver_sm failed for DLR with message id {}: {}",
                    correlationKey, proxyResponse.errorMessage());
            throw new SmsDeliveryException(proxyResponse.toString(), mediaType);
        }

        log.debug("Proxy mode: deliver_sm confirmed for DLR with message id {}", correlationKey);
    }

    private boolean isProxySubmit(UtilsRecords.SubmitSmResponseEvent submitResult) {
        return Objects.nonNull(submitResult)
                && Objects.nonNull(submitResult.customParams())
                && Boolean.parseBoolean(String.valueOf(submitResult.customParams().get(PROXY_MODE_CUSTOM_PARAM)));
    }

    private Map<String, Object> processResponseDlrMO(MessageEvent messageEvent, String systemId, Gateway gateway) {
        messageEvent.setShortMessage("DLR sent successfully");
        messageEvent.setSystemId(gateway.getSystemId());
        PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), RESPONSE_DLR_EVENT_TYPE, OUTPUT_DIRECTION);

        if (Objects.nonNull(payloadMapper)) {
            String bodyResponse = InterpreterUtils.interpreterFormatForMT(messageEvent, payloadMapper);
            Map<String, Object> response = InterpreterUtils.createResponseByBodyType(bodyResponse, payloadMapper.getBodyType());

            Map<String, String> headers = new HashMap<>();
            InterpreterUtils.forEachInterpreterHeader(
                    gateway,
                    messageEvent,
                    OUTPUT_DIRECTION,
                    RESPONSE_DLR_EVENT_TYPE,
                    headers::put
            );

            if (!headers.isEmpty()) {
                response.put("headers", headers);
            }

            return response;
        }

        log.error("No interpreter found for dlr, system id {}", systemId);
        throw new NoInterpreterFoundException("No interpreter found for response dlr", this.mediaType);
    }

    private String getMessageId(Map<String, Object> bodyRequest) {
        Object messageId = bodyRequest.get("message_id");
        if (Objects.isNull(messageId)) {
            messageId = bodyRequest.get("messageId");
        }
        return Objects.nonNull(messageId) ? messageId.toString() : "";
    }
}
