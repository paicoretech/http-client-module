package com.client.http.utils;

import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.interpreter.EventPlaceholderResolver;
import com.paicbd.smsc.interpreter.PayloadFormat;
import com.paicbd.smsc.interpreter.PayloadInterpreter;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.utils.Converter;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.client.http.utils.Constants.IS_STARTED;

@Slf4j
public class InterpreterUtils {
    private InterpreterUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static PayloadMapper findInterpreterByEventType(List<PayloadMapper> interpreters, String eventType, String direction) {
        if (Objects.isNull(interpreters)) {
            return null;
        }

        return interpreters.stream()
                .filter(interpreter -> interpreter.getEventType().equals(eventType) && interpreter.getDirection().equals(direction))
                .findFirst()
                .orElse(null);
    }

    public static String interpreterFormatForMT(MessageEvent messageEvent, PayloadMapper payloadMapper) {
        try {
            if (Objects.nonNull(payloadMapper)) {
                String bodyType = payloadMapper.getBodyType().toUpperCase();
                return PayloadInterpreter.interpretPayloadForSend(payloadMapper.getTemplate(), messageEvent, PayloadFormat.valueOf(bodyType));
            }
        } catch (Exception e) {
            log.error("Error to convert submitSmEvent -> {}" ,e.getMessage(), e);
        }

        return "";
    }

    public static void interpreterFormatForMO(MessageEvent messageEvent, Object bodyRequestObject, PayloadMapper payloadMapper) {
        try {
            if (Objects.nonNull(payloadMapper)) {
                String bodyType = payloadMapper.getBodyType().toUpperCase();
                String bodyRequest = converterObjectToString(bodyRequestObject, bodyType);
                PayloadInterpreter.interpreterPayloadForReceive(bodyRequest, payloadMapper.getTemplate(), messageEvent, PayloadFormat.valueOf(bodyType));
            }
        } catch (Exception e) {
            log.error("Error to convert submitSmEvent -> {}" ,e.getMessage(), e);
        }
    }

    public static String createKeyToMapForPath(String eventType, String path) {
        return eventType + path;
    }

    public static boolean gatewayIsInvalid(Gateway gateway) {
        if (Objects.isNull(gateway)) {
            log.error("Gateway was not found to process message MO");
            return true;
        }

        if (gateway.getEnabled() != IS_STARTED) {
            log.warn("Gateway {} is not enabled", gateway.getSystemId());
            return true;
        }

        return false;
    }

    public static String converterObjectToString(Object bodyRequestObject, String bodyType) {
        if (bodyRequestObject instanceof String) {
            return bodyRequestObject.toString();
        }

        if (bodyType.equalsIgnoreCase(PayloadFormat.XML.name())) {
            return Converter.valueAsStringXML(bodyRequestObject);
        }
        return Converter.valueAsString(bodyRequestObject);
    }

    public static Map<String, Object> createResponseByBodyType(String bodyRequest, String bodyType) {
        Map<String, Object> response = new HashMap<>();

        if (bodyType.equalsIgnoreCase(PayloadFormat.XML.name())) {
            response.put("contentType", "application/xml");
            response.put("body", bodyRequest);
        } else {
            response.put("contentType", "application/json");
            response.put("body", Converter.stringToObject(bodyRequest, Object.class));
        }

        return response;
    }

    public static void forEachInterpreterHeader(
            Gateway gw,
            MessageEvent event,
            String direction,
            String eventType,
            BiConsumer<String, String> headerAction
    ) {
        if (gw == null || headerAction == null) return;

        var callbackHeaders = EventPlaceholderResolver.getCallbackHeadersFor(
                gw.getInterpreter(), direction, eventType);

        callbackHeaders.stream()
                .filter(Objects::nonNull)
                .map(ch -> Map.entry(
                        ch.getHeaderName(),
                        EventPlaceholderResolver.replaceEventPlaceholder(ch.getHeaderValue(), event)
                ))
                .filter(e -> e.getKey() != null && e.getValue() != null && !e.getValue().isBlank())
                .forEach(e -> headerAction.accept(e.getKey(), e.getValue()));
    }
}
