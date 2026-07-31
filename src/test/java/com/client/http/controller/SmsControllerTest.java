package com.client.http.controller;

import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.exception.SmsProcessingException;
import com.client.http.http.GatewayHttpConnection;
import com.client.http.service.MessageEventService;
import com.client.http.service.ProcessMessageDlrService;
import com.client.http.service.ProcessMessageService;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import reactor.core.publisher.Mono;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SmsControllerTest {

    @Mock
    private MessageEventService messageEventService;

    @InjectMocks
    private SmsController smsController;

    @Mock
    ProcessMessageDlrService processMessageDlrService;

    @Mock
    ProcessMessageService processMessageService;

    @Mock
    ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap;

    @Mock
    ConcurrentMap<String, Gateway> gatewayConcurrentHashMap;

    @Mock
    ConcurrentMap<String, GatewayHttpConnection> httpConnectionManagerList;

    @Mock
    ScyllaManager scyllaManager;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    com.client.http.utils.AppProperties appProperties;

    @Test
    @DisplayName("Starting the request watcher")
    void smsControllerWhenStartWatcherThenExecutedPostConstruct() {
        SmsController spySmsController = spy(smsController);
        spySmsController.init();
        verify(spySmsController).init();
    }

    @ParameterizedTest
    @SuppressWarnings("unchecked")
    @DisplayName("Sending DLR when messageId not found then SmsProcessingException and JSON content type is receiver")
    @ValueSource(strings = {"json", "xml"})
    void deliveryWhenSendDLRThenResponseIsOK(String bodyType) {
        processMessageDlrService = new ProcessMessageDlrService(interpreterByPathRequestConcurrentHashMap, gatewayConcurrentHashMap, httpConnectionManagerList, scyllaManager, kafkaTemplate, new com.client.http.components.ProxyResponseHandler(), appProperties);
        this.messageEventService = new MessageEventService(processMessageDlrService, processMessageService);
        smsController = new SmsController(messageEventService);

        MediaType mediaType = MediaType.valueOf("application/" + bodyType);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("message_id", "1719421854353-11028072268459");
        requestBody.put("source_addr_ton", 1);
        requestBody.put("source_addr", "50510201020");
        requestBody.put("destination_addr_ton", 1);
        requestBody.put("destination_addr", "50582368999");
        requestBody.put("status", "UNDELIV");
        requestBody.put("error_code", "500");

        MockHttpServletRequest mockRequest = new MockHttpServletRequest();
        mockRequest.setRequestURI("/message");
        mockRequest.addHeader("Content-Type", mediaType);

        Mono<ResponseEntity<Object>> responseMono = smsController.delivery(requestBody, mockRequest)
                .onErrorResume(SmsProcessingException.class, e -> Mono.just(ResponseEntity
                        .status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .contentType(e.getMediaType())
                        .body(e.getResponse())));

        ResponseEntity<Object> result = responseMono.block();
        assertNotNull(result);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getStatusCode());
        assertNotNull(result.getBody());
        Map<String, Object> responseBody;
        if ("xml".equals(bodyType)) {
            responseBody = Converter.stringXMLToObject(String.valueOf(result.getBody()), Map.class);
        } else {
            responseBody = (Map<String, Object>) result.getBody();
        }

        assertEquals("error", responseBody.get("status"));
        assertEquals(mediaType, result.getHeaders().getContentType());
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }
}
