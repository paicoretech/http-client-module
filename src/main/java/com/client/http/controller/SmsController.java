package com.client.http.controller;

import com.client.http.exception.NoInterpreterFoundException;
import com.client.http.exception.SmsDeliveryException;
import com.client.http.exception.SmsProcessingException;
import com.client.http.service.MessageEventService;
import com.paicbd.smsc.utils.Watcher;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@RestController
@RequiredArgsConstructor
public class SmsController {
    private final AtomicInteger requestPerSecond = new AtomicInteger(0);
    private final MessageEventService messageEventService;

    @PostConstruct
    public void init() {
        Thread.startVirtualThread(() -> new Watcher("HTTP-Submit-Received", requestPerSecond, 1));
    }

    @PostMapping("/**")
    public Mono<ResponseEntity<Object>> delivery(@RequestBody Map<String, Object> bodyRequest, HttpServletRequest request) {
        return Mono.fromCallable(() -> {
            requestPerSecond.incrementAndGet();
            Map<String, Object> response = messageEventService.processRequest(bodyRequest, request.getRequestURI(), request.getContentType());
            String contentType = response.getOrDefault("contentType", MediaType.APPLICATION_JSON_VALUE).toString();

            HttpHeaders headers = new HttpHeaders();
            Object headersObj = response.get("headers");
            if (headersObj instanceof Map<?, ?> raw) {
                Map<String, String> headerMap = new HashMap<>();
                raw.forEach((k, v) -> {
                    if (k instanceof String key && v instanceof String value) {
                        headerMap.put(key, value);
                    }
                });
                headers.setAll(headerMap);
            }

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentType(MediaType.valueOf(contentType))
                    .body(response.getOrDefault("body", ""));
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @ExceptionHandler(SmsDeliveryException.class)
    public ResponseEntity<Object> handleSmsDeliveryException(SmsDeliveryException e) {
        return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT)
                .contentType(e.getMediaType()).body(e.getResponse());
    }

    @ExceptionHandler(NoInterpreterFoundException.class)
    ResponseEntity<Object> handleSmsProcessInterpreterException(NoInterpreterFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .contentType(e.getMediaType()).body(e.getResponse());
    }

    @ExceptionHandler(SmsProcessingException.class)
    ResponseEntity<Object> handleSmsProcessException(SmsProcessingException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(e.getMediaType()).body(e.getResponse());
    }
}
