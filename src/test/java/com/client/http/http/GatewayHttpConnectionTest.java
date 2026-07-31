package com.client.http.http;

import com.client.http.utils.AppProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.UtilsEnum;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GatewayHttpConnectionTest {

    @Mock
    ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;

    @Mock
    AppProperties appProperties;

    @Mock
    RedisManager redisManager;

    @Mock
    Gateway gateway;

    GatewayHttpConnection gatewayHttpConnection;

    @Mock
    HttpResponse<String> response;

    @Mock
    ScyllaManager scyllaManager;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    KafkaConsumer<String, String> kafkaCon;

    @Mock
    private KafkaConsumerFactory kafkaConsumerFactory;

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    @DisplayName("Send message and response not contains the message id")
    void sendMessageWhenValidateRegisteredDeliveryThenDoNothing(int registeredDelivery) {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setInterpreter(null);
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setRegisteredDelivery(registeredDelivery);
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock-> ConsumerRecords.empty());
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(2);
        verify(scyllaManager, never()).insertIntoTable(eq(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE), anyString(), anyString());
        gatewayHttpConnection.stopConnection();
    }

    @Test
    @DisplayName("Send message when message parts is not null")
    void sendMessageWhenMessagePartsIsNotNullThenDoNothing() {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("JSON");
        String messageId = "1719421854353-11028072268459";
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setEsmClass(64);
        messageEvent.setMessageParts(
                List.of(
                        MessagePart.builder()
                                .messageId(messageId)
                                .shortMessage("Testing message part I'm the first part")
                                .segmentSequence(1)
                                .totalSegment(2)
                                .msgReferenceNumber("2")
                                .build(),
                        MessagePart.builder()
                                .messageId(messageId)
                                .shortMessage("Testing message part I'm the second part")
                                .segmentSequence(2)
                                .totalSegment(2)
                                .msgReferenceNumber("2")
                                .build()
                )
        );
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock->ConsumerRecords.empty());
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(2);
        verify(scyllaManager, never()).insertIntoTable(eq(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE), anyString(), anyString());
        gatewayHttpConnection.stopConnection();
    }

    @ParameterizedTest
    @MethodSource("handlerErrorParameters")
    @DisplayName("Error handler when throws HttpTimeoutException")
    void globalErrorHandlerWhenThrowsExceptionThenSendToAutoRetryProcess(int validityPeriod, boolean lastRetry, String originProtocol) {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("XML");
        httpGW.setPduTimeout(100);
        httpGW.setIp("http://18.224.164.86:3000/api/callback");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setValidityPeriod(validityPeriod);
        messageEvent.setLastRetry(lastRetry);
        messageEvent.setOriginProtocol(originProtocol);
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock->ConsumerRecords.empty());
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(4);

        if (!originProtocol.isEmpty()) {
            verify(kafkaTemplate).send(eq("cdr"), anyString());
        } else {
            verify(kafkaTemplate, never()).send(eq(originProtocol), anyString());
        }
        gatewayHttpConnection.stopConnection();
    }

    @ParameterizedTest
    @ValueSource(ints = {404, 408})
    @DisplayName("Handle no retry error")
    void handleNoRetryErrorWhenErrorInNoRetryListThenCheckValues(int errorCode) {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("XML");
        httpGW.setPduTimeout(100);
        httpGW.setIp("http://18.224.164.86:3000/api/callback");
        httpGW.setNoRetryErrorCode("408");

        List<PayloadMapper> payloadMappers;
        if (errorCode == 404) {
            payloadMappers = getPayloadMappers("XML", false);
        } else {
            payloadMappers = getPayloadMappers("JSON", false);
        }
        httpGW.setInterpreter(payloadMappers);

        when(appProperties.isHttp2()).thenReturn(true);
        MessageEvent messageEvent = getMessageEvent();
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock-> ConsumerRecords.empty());
        List<ErrorCodeMapping> errorCodeMappings = getErrorCodeMappingList(errorCode);
        when(errorCodeMappingConcurrentHashMap.get(String.valueOf(httpGW.getMno()))).thenReturn(errorCodeMappings);
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(2);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), valueCaptor.capture());
        MessageEvent deliverSmEvent = Converter.stringToObject(valueCaptor.getValue(), MessageEvent.class);
        assertEquals(messageEvent.getMessageId(), deliverSmEvent.getMessageId());
        gatewayHttpConnection.stopConnection();
    }

    @Test
    @DisplayName("DLR short message excludes original text when HTTP delivery fails for SMPP-origin message")
    void createDeliverSmWhenHttpFailureForSmppOriginThenDlrShortMessageExcludesOriginalText() {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setPduTimeout(100);
        httpGW.setIp("http://18.224.164.86:3000/api/callback");
        httpGW.setNoRetryErrorCode("408");
        when(appProperties.isHttp2()).thenReturn(true);

        String longMessage = """
                This is a test message designed to trigger multi-part SMS concatenation in the SMSC. \
                It is intentionally long so that it exceeds the standard 160-character GSM 7-bit \
                single-segment limit and forces the system to split it into multiple parts using UDH. \
                Each segment should carry exactly 153 characters of user data, with the remaining 7 \
                octets reserved for the concatenation UDH header. If you are reading this as a single \
                message, reassembly worked correctly on the receiving end.""";
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setOriginProtocol("SMPP");
        messageEvent.setShortMessage(longMessage);

        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock -> ConsumerRecords.empty());
        List<ErrorCodeMapping> errorCodeMappings = getErrorCodeMappingList(408);
        when(errorCodeMappingConcurrentHashMap.get(String.valueOf(httpGW.getMno()))).thenReturn(errorCodeMappings);
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(2);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), valueCaptor.capture());
        MessageEvent deliverSmEvent = Converter.stringToObject(valueCaptor.getValue(), MessageEvent.class);
        assertEquals(messageEvent.getMessageId(), deliverSmEvent.getMessageId());
        assertFalse(deliverSmEvent.getShortMessage().contains(longMessage));
        assertTrue(deliverSmEvent.getShortMessage().length() <= 254);
        gatewayHttpConnection.stopConnection();
    }

    @Test
    @DisplayName("Handle auto retry")
    void handleAutoRetryWhenErrorInAutoRetryListThenCheckValues() {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setPduTimeout(100);
        httpGW.setIp("http://18.224.164.86:3000/api/callback");
        httpGW.setAutoRetryErrorCode("408");
        when(appProperties.isHttp2()).thenReturn(true);
        MessageEvent messageEvent = getMessageEvent();
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock -> ConsumerRecords.empty());
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(3);

        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.RETRIES_MEDIUM_TOPIC), valueCaptor.capture());
        MessageEvent submitSmEventToRetry = Converter.stringToObject(valueCaptor.getValue(), MessageEvent.class);
        assertEquals(messageEvent.getMessageId(), submitSmEventToRetry.getMessageId());
        assertEquals(1, submitSmEventToRetry.getRetryNumber());
        gatewayHttpConnection.stopConnection();
    }

    @Test
    @DisplayName("Add submit sm response in cache")
    void addInCacheWhenRegisteredDeliveryIsNotZeroThenCheckValues() throws Exception {
        MessageEvent messageEvent = MessageEvent.builder().messageId("1719421854355-110280722684595").build();
        when(response.body()).thenReturn(messageEvent.toString());
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());

        MessageEvent submitSmEvent = getMessageEvent();

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class}, submitSmEvent, response);

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> fieldCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> smResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(scyllaManager).insertIntoTable(keyCaptor.capture(), fieldCaptor.capture(), smResponseCaptor.capture());

        assertEquals(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, keyCaptor.getValue());
        assertEquals(messageEvent.getMessageId(), fieldCaptor.getValue());
        UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = Converter.stringToObject(smResponseCaptor.getValue(), UtilsRecords.SubmitSmResponseEvent.class);
        assertEquals(messageEvent.getMessageId(), submitSmResponseEvent.hashId());
        assertEquals(submitSmEvent.getSystemId(), submitSmResponseEvent.systemId());
    }

    @Test
    @DisplayName("addInCache with query params mode includes response body as CDR comment")
    void addInCacheQueryParamsModeIncludesResponseBodyInCdr() throws Exception {
        String gatewayResponse = "{\"code\":\"failed\",\"description\":\"Login failed\",\"message_id\":null}";
        when(response.body()).thenReturn(gatewayResponse);
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());
        when(gateway.getIp()).thenReturn("http://10.200.216.43:8085/proxy_selcom/?__mode=qp");

        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setRegisteredDelivery(0);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class},
                submitSmEvent, response);

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

        UtilsRecords.Cdr cdr = Converter.stringToObject(cdrCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals(gatewayResponse, cdr.comment());
        assertEquals(UtilsEnum.CdrStatus.SUCCESS.name(), cdr.status());
    }

    @Test
    @DisplayName("addInCache without query params mode has empty CDR comment")
    void addInCacheNonQueryParamsModeHasEmptyCommentInCdr() throws Exception {
        MessageEvent responseMsg = MessageEvent.builder().messageId("1719421854355-110280722684595").build();
        when(response.body()).thenReturn(responseMsg.toString());
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());
        when(gateway.getIp()).thenReturn("http://18.224.164.85:3000/api/callback");

        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setRegisteredDelivery(0);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class},
                submitSmEvent, response);

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

        UtilsRecords.Cdr cdr = Converter.stringToObject(cdrCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals("", cdr.comment());
        assertEquals(UtilsEnum.CdrStatus.SUCCESS.name(), cdr.status());
        // mno_message_id carries the gateway-assigned id from the response even without a DLR request.
        assertEquals(responseMsg.getMessageId(), cdr.mnoMessageId());
    }

    @Test
    @DisplayName("addInCache query params mode compacts pretty-printed JSON response")
    void addInCacheQueryParamsModeCompactsPrettyPrintedJson() throws Exception {
        String prettyPrintedResponse = "{\n  \"message\": \"OK\",\n  \"message_id\": 1774464608318288686,\n  \"status\": 200\n}";
        String expectedCompact = "{\"message\":\"OK\",\"message_id\":1774464608318288686,\"status\":200}";
        when(response.body()).thenReturn(prettyPrintedResponse);
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());
        when(gateway.getIp()).thenReturn("http://10.200.216.43:8085/proxy_selcom/?__mode=qp");

        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setRegisteredDelivery(0);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class},
                submitSmEvent, response);

        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());

        UtilsRecords.Cdr cdr = Converter.stringToObject(cdrCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals(expectedCompact, cdr.comment());
        assertEquals(UtilsEnum.CdrStatus.SUCCESS.name(), cdr.status());
    }

    @Test
    @DisplayName("Send message with a invalid template body")
    void sendMessageWhenTemplateFormatIsInvalid() {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setPduTimeout(100);
        httpGW.setIp("http://18.224.164.86:3000/api/callback");
        List<PayloadMapper> payloadMappers = getPayloadMappers("JSON", true);
        httpGW.setInterpreter(payloadMappers);

        when(appProperties.isHttp2()).thenReturn(true);
        MessageEvent messageEvent = getMessageEvent();
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        when(kafkaCon.poll(any(Duration.class)))
                .thenReturn(fromSingleRecord(messageEvent.toString()))
                .thenAnswer(invocationOnMock -> ConsumerRecords.empty());
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();

        toSleep(2);
        verify(kafkaCon, atLeastOnce()).poll(any(Duration.class));
        gatewayHttpConnection.stopConnection();
    }

    @Test
    @DisplayName("Send message in query params mode returns success and publishes proxy response")
    void sendMessageQueryParamsModeWhenHttpSuccessThenCachesAndPublishesProxyResponse() throws Exception {
        AtomicReference<String> requestedUrl = new AtomicReference<>();
        MessageEvent gatewayResponse = MessageEvent.builder().messageId("gw-123").build();
        HttpServer server = startHttpServer(200, gatewayResponse.toString(), requestedUrl);
        try {
            Gateway httpGW = getHTTPGw("JSON");
            httpGW.setInterpreter(getPayloadMappersDefault());
            httpGW.setIp("http://localhost:" + server.getAddress().getPort() + "/api?__mode=qp");
            httpGW.setPduTimeout(2000);
            MessageEvent messageEvent = getMessageEvent();
            messageEvent.setUseProxy(true);
            messageEvent.setShortMessage("Hello Proxy");
            messageEvent.setCustomParams(new HashMap<>(Map.of("system_id", "httpgw", "pwd", "1234")));

            gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                    errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
            invokePrivateMethodByName(gatewayHttpConnection, "sendMessage", new Class<?>[]{MessageEvent.class}, messageEvent);

            assertEquals("/api?user=httpgw&pwd=1234&senderid=50510201020&phone=50582368999&msgtext=Hello+Proxy",
                    requestedUrl.get());
            verify(scyllaManager).insertIntoTable(eq(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE), eq("GW-123"), anyString());
            ArgumentCaptor<String> proxyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(KafkaTopicsConstants.HTTP_PROXY_TOPIC), proxyCaptor.capture());
            UtilsRecords.HttpProxyResponse proxyResponse = Converter.stringToObject(proxyCaptor.getValue(), UtilsRecords.HttpProxyResponse.class);
            assertEquals(messageEvent.getMessageId(), proxyResponse.messageId());
            assertFalse(proxyResponse.error());
            assertEquals("gw-123", proxyResponse.gatewayMessageId());
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("Send message in SELCOM mode returns server error and publishes failed proxy response")
    void sendMessageSelcomModeWhenHttpErrorThenPublishesFailedProxyResponse() throws Exception {
        AtomicReference<String> requestedUrl = new AtomicReference<>();
        HttpServer server = startHttpServer(500, "{\"message\":\"failure\"}", requestedUrl);
        try {
            Gateway httpGW = getHTTPGw("JSON");
            httpGW.setIp("http://localhost:" + server.getAddress().getPort() + "/api?__mode=sel");
            httpGW.setPduTimeout(2000);
            MessageEvent messageEvent = getMessageEvent();
            messageEvent.setRegisteredDelivery(0);
            messageEvent.setValidityPeriod(0);
            messageEvent.setUseProxy(true);
            messageEvent.setShortMessage("Hello Selcom");
            messageEvent.setCustomParams(Map.of("system_id", "httpgw", "pwd", "1234"));

            gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                    errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
            invokePrivateMethodByName(gatewayHttpConnection, "sendMessage", new Class<?>[]{MessageEvent.class}, messageEvent);

            assertEquals("/api?USERNAME=httpgw&PASSWORD=1234&DESTADDR=50582368999&MESSAGE=Hello+Selcom",
                    requestedUrl.get());
            ArgumentCaptor<String> proxyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(KafkaTopicsConstants.HTTP_PROXY_TOPIC), proxyCaptor.capture());
            UtilsRecords.HttpProxyResponse proxyResponse = Converter.stringToObject(proxyCaptor.getValue(), UtilsRecords.HttpProxyResponse.class);
            assertEquals(messageEvent.getMessageId(), proxyResponse.messageId());
            assertTrue(proxyResponse.error());
            assertEquals(500, proxyResponse.errorCode());
        } finally {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("Add in cache marks submit response custom params when proxy mode is enabled")
    void addInCacheWhenUseProxyThenStoresProxyModeCustomParam() throws Exception {
        MessageEvent responseMsg = MessageEvent.builder().messageId("proxy-cache-id").build();
        when(response.body()).thenReturn(responseMsg.toString());
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());

        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setUseProxy(true);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class}, submitSmEvent, response);

        ArgumentCaptor<String> smResponseCaptor = ArgumentCaptor.forClass(String.class);
        verify(scyllaManager).insertIntoTable(eq(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE), eq("PROXY-CACHE-ID"), smResponseCaptor.capture());
        UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = Converter.stringToObject(smResponseCaptor.getValue(), UtilsRecords.SubmitSmResponseEvent.class);
        assertEquals("true", String.valueOf(submitSmResponseEvent.customParams().get("use_proxy")));
    }

    @Test
    @DisplayName("New request includes gateway authentication header when configured")
    void newRequestWhenAuthenticationConfiguredThenAddsSecurityHeader() throws Exception {
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setAuthenticationTypes("Bearer");
        httpGW.setHeaderSecurityName("Authorization");
        httpGW.setToken("token-123");

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        HttpRequest request = (HttpRequest) invokePrivateMethodByName(gatewayHttpConnection, "newRequest",
                new Class<?>[]{String.class, String.class, Gateway.class, MessageEvent.class},
                "{}", "application/json", httpGW, getMessageEvent());

        assertEquals("token-123", request.headers().firstValue("Authorization").orElse(""));
    }

    @Test
    @DisplayName("Retry alternate destination sends message to routing topic and increments retry number")
    void sendToRetryProcessWhenErrorInAlternateDestinationListThenRoutesMessage() throws Exception {
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setRetryAlternateDestinationErrorCode("502");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setRetryNumber(2);
        messageEvent.setErrorCode(502);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethodByName(gatewayHttpConnection, "sendToRetryProcess", new Class<?>[]{MessageEvent.class, int.class}, messageEvent, 502);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2)).send(topicCaptor.capture(), payloadCaptor.capture());
        MessageEvent retriedEvent = Converter.stringToObject(payloadCaptor.getAllValues().get(1), MessageEvent.class);
        assertTrue(retriedEvent.isRetry());
        assertEquals(3, retriedEvent.getRetryNumber());
    }

    @Test
    @DisplayName("Publish in Kafka topic handles message, DLR, and send failures")
    void publishInKafkaTopicWhenMessageTypeChangesThenUsesExpectedRouting() {
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, getHTTPGw("JSON"),
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);

        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setDestProtocol("SMPP");
        gatewayHttpConnection.publishInKafkaTopic(messageEvent);

        MessageEvent dlrEvent = getMessageEvent();
        dlrEvent.setDestProtocol("SMPP");
        dlrEvent.setDlr(true);
        dlrEvent.setProcess(true);
        gatewayHttpConnection.publishInKafkaTopic(dlrEvent);

        doThrow(new RuntimeException("send failed")).when(kafkaTemplate).send(anyString(), anyString());
        MessageEvent failureEvent = getMessageEvent();
        failureEvent.setDestProtocol("SMPP");
        gatewayHttpConnection.publishInKafkaTopic(failureEvent);

        verify(kafkaTemplate, times(3)).send(anyString(), anyString());
    }

    @Test
    @DisplayName("addInCache logs warning and skips cache when DLR requested but response has no message id")
    void addInCacheWhenDlrRequestedButResponseHasNoMessageIdThenSkipsCache() throws Exception {
        // Response body without a message_id so the interpreter yields a blank gatewayMessageId
        when(response.body()).thenReturn("{}");
        when(gateway.getInterpreter()).thenReturn(getPayloadMappersDefault());

        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setRegisteredDelivery(RequestDelivery.REQUEST_DLR.getValue());

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, gateway,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethod(gatewayHttpConnection, new Class<?>[]{MessageEvent.class, HttpResponse.class}, submitSmEvent, response);

        // No cache entry is created because there is no gateway-assigned message id
        verify(scyllaManager, never()).insertIntoTable(eq(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE), anyString(), anyString());
        // The CDR is still published with a null mno_message_id
        ArgumentCaptor<String> cdrCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CDR_TOPIC), cdrCaptor.capture());
        UtilsRecords.Cdr cdr = Converter.stringToObject(cdrCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals(UtilsEnum.CdrStatus.SUCCESS.name(), cdr.status());
    }

    @Test
    @DisplayName("Handler failed message applies refund to charging topic when message is refundable")
    void handlerFailedMessageWhenApplyForRefundThenSendsToChargingTopic() throws Exception {
        Gateway httpGW = getHTTPGw("JSON");
        MessageEvent submitSmEvent = getMessageEvent();
        submitSmEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
        submitSmEvent.setApplyForRefund(true);
        submitSmEvent.setErrorCode(500);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        invokePrivateMethodByName(gatewayHttpConnection, "handlerFailedMessage",
                new Class<?>[]{MessageEvent.class, int.class}, submitSmEvent, 500);

        ArgumentCaptor<String> chargingCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaTopicsConstants.CHARGING_MEDIUM_MESSAGE_TOPIC), chargingCaptor.capture());
        MessageEvent refundEvent = Converter.stringToObject(chargingCaptor.getValue(), MessageEvent.class);
        assertEquals(submitSmEvent.getMessageId(), refundEvent.getMessageId());
    }

    @Test
    @DisplayName("Send message handles IOException as a 500 error and publishes failed proxy response")
    void sendMessageWhenIOExceptionThenGlobalErrorHandlerReportsServerError() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();
            AtomicInteger requestBytesRead = new AtomicInteger(-1);
            Thread acceptThread = startAbruptCloseServer(serverSocket, requestBytesRead);

            Gateway httpGW = getHTTPGw("JSON");
            httpGW.setIp("http://localhost:" + port + "/api");
            httpGW.setPduTimeout(2000);
            httpGW.setNoRetryErrorCode("500");

            MessageEvent messageEvent = getMessageEvent();
            messageEvent.setRegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue());
            messageEvent.setValidityPeriod(0);
            messageEvent.setUseProxy(true);

            gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                    errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
            invokePrivateMethodByName(gatewayHttpConnection, "sendMessage", new Class<?>[]{MessageEvent.class}, messageEvent);

            acceptThread.join(2000);
            assertTrue(requestBytesRead.get() > 0, "The gateway should have received the HTTP request before closing");

            ArgumentCaptor<String> proxyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(KafkaTopicsConstants.HTTP_PROXY_TOPIC), proxyCaptor.capture());
            UtilsRecords.HttpProxyResponse proxyResponse = Converter.stringToObject(proxyCaptor.getValue(), UtilsRecords.HttpProxyResponse.class);
            assertEquals(messageEvent.getMessageId(), proxyResponse.messageId());
            assertTrue(proxyResponse.error());
            assertEquals(500, proxyResponse.errorCode());
        }
    }

    private static Thread startAbruptCloseServer(ServerSocket serverSocket, AtomicInteger requestBytesRead) {
        Thread acceptThread = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                // Consume the request bytes and then close abruptly so the HTTP client fails
                // reading the response with a plain IOException (not a timeout nor a connect error)
                requestBytesRead.set(socket.getInputStream().read(new byte[2048]));
            } catch (IOException ignored) {
                // expected: connection closed abruptly
            }
        });
        acceptThread.setDaemon(true);
        acceptThread.start();
        return acceptThread;
    }

    private static HttpServer startHttpServer(int statusCode, String responseBody, AtomicReference<String> requestedUrl) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/api", exchange -> {
            requestedUrl.set(exchange.getRequestURI().toString());
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return server;
    }

    private static Stream<Arguments> handlerErrorParameters() {
        return Stream.of(
                Arguments.of(0, false, "http"),
                Arguments.of(320, true, "smpp"),
                Arguments.of(0, false, "")
        );
    }

    private static void invokePrivateMethod(Object targetObject, Class<?>[] parameterTypes, Object... parameters)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = targetObject.getClass().getDeclaredMethod("addInCache", parameterTypes);
        method.setAccessible(true);
        method.invoke(targetObject, parameters);
    }

    private static Object invokePrivateMethodByName(Object targetObject, String methodName, Class<?>[] parameterTypes, Object... parameters)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = targetObject.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(targetObject, parameters);
    }

    private static Gateway getHTTPGw(String bodyType) {
        List<PayloadMapper> payloadMappers = getPayloadMappers(bodyType, false);

        return Gateway.builder()
                .networkId(3)
                .name("httpgw")
                .systemId("httpgw")
                .password("1234")
                .ip("http://18.224.164.85:3000/api/callback")
                .port(9409)
                .bindType("TRANSCEIVER")
                .systemType("")
                .interfaceVersion("IF_50")
                .sessionsNumber(1)
                .addressTON(1)
                .addressNPI(4)
                .addressRange("")
                .tps(1)
                .messagesPerSecond(1)
                .messagesPerSecondMedium(1)
                .status("STOPPED")
                .enabled(0)
                .enquireLinkPeriod(30000)
                .enquireLinkTimeout(0)
                .requestDLR(RequestDelivery.NON_REQUEST_DLR.getValue())
                .noRetryErrorCode("400, 500")
                .retryAlternateDestinationErrorCode("502")
                .bindTimeout(5000)
                .bindRetryPeriod(10000)
                .pduTimeout(5000)
                .pduProcessorDegree(1)
                .threadPoolSize(100)
                .mno(1)
                .tlvMessageReceiptId(false)
                .sessionsNumber(0)
                .protocol("HTTP")
                .autoRetryErrorCode("404")
                .encodingIso88591(3)
                .encodingGsm7(1)
                .encodingUcs2(2)
                .splitMessage(false)
                .splitSmppType("TLV")
                .interpreter(payloadMappers)
                .build();
    }

    private static MessageEvent getMessageEvent() {
        return MessageEvent.builder()
                .id("1719421854353")
                .messageId("1719421854353-11028072268459")
                .registeredDelivery(1)
                .originNetworkId(6)
                .systemId("httpgw")
                .deliverSmId("1")
                .sourceAddrNpi(1)
                .sourceAddrTon(1)
                .sourceAddr("50510201020")
                .originProtocol("HTTP")
                .routingId(3)
                .retryDestNetworkId("")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .validityPeriod(160)
                .smscMessagePriority(GeneralSmscConstants.MEDIUM_PRIORITY)
                .build();
    }



    private static List<ErrorCodeMapping> getErrorCodeMappingList(Integer errorCode) {
        return List.of(ErrorCodeMapping.builder().errorCode(errorCode).deliveryErrorCode(55).deliveryStatus("DELIVRD").build());
    }

    static void toSleep(long seconds) {
        try (ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor()) {
            executorService.schedule(() -> {
            }, seconds, TimeUnit.SECONDS);
        }
    }

    private static List<PayloadMapper> getPayloadMappers(String bodyType, boolean invalidPayload) {
        List<PayloadMapper> payloadMappers = new ArrayList<>();

        PayloadMapper payloadMapper = new PayloadMapper();
        payloadMapper.setBodyType(bodyType);
        payloadMapper.setDirection("output");
        payloadMapper.setEventType("message");

        String bodyString;

        if ("XML".equals(bodyType)) {
            bodyString = """
                <smpp>
                	<commandId>4</commandId>
                	<commandLength>47</commandLength>
                	<sequenceNumber>6</sequenceNumber>
                	<serviceType></serviceType>
                	<sourceAddress>
                		<address>{{sourceAddr:STRING}}</address>
                		<ton>{{sourceAddrTon:HEX}}</ton>
                		<npi>{{sourceAddrNpi:HEX}}</npi>
                	</sourceAddress>
                	<destAddress>
                		<address>{{destinationAddr:LIST}}</address>
                		<ton>{{destAddrTon:HEX}}</ton>
                		<npi>{{destAddrNpi:HEX}}</npi>
                	</destAddress>
                	<scheduleDeliveryTime></scheduleDeliveryTime>
                	<validityPeriod></validityPeriod>
                	<dataCoding>{{dataCoding:HEX}}</dataCoding>
                	<protocolId>0x00</protocolId>
                	<priority>0x00</priority>
                	<registerDelivery>{{registeredDelivery:HEX}}</registerDelivery>
                	<replaceIfPresent>0x00</replaceIfPresent>
                	<messageLength>6</messageLength>
                	<message>{{shortMessage:STRING}}</message>
                	<clientId>{{systemId:STRING}}</clientId>
                	<host>127.0.0.1</host>
                </smpp>
                """;
        } else {
            if (invalidPayload) {
                bodyString = """
                {
                  "commandId": "{{commandId:INT}}",
                  "commandLength": 47,
                  "sequenceNumber": 6,
                  "serviceType": "",
                  "sourceAddress": {
                    "address": "{{sourceAddr:STRING}}",
                    "ton": "{{sourceAddrTon:HEX}}",
                    "npi": "{{sourceAddrNpi:HEX}}"
                  },
                  "destAddress": {
                    "address": "{{destinationAddr:LIST}}",
                    "ton": "{{destAddrTon:INT}}",
                    "npi": "{{destAddrNpi:HEX}}"
                  },
                  "scheduleDeliveryTime": "",
                  "validityPeriod": "",
                  "priority": "0x00",
                  "registerDelivery": "{{registeredDelivery:BOOLEAN}}",
                  "replaceIfPresent": "0x00",
                  "messageLength": 6,
                  "message": "{{shortMessage:STRING}}",
                  "clientId": "{{systemId:STRING}}",
                  "host": "127.0.0.1",
                  "esmClass": "{{esmClass:HEX}}",
                  "optParams": "{{optionalParameters:LIST}}",
                  "isDlr": "{{isDlr:HEX}}"
                }
                """;
            } else {
                bodyString = """
                {
                  "commandId": "{{commandId:INT}}",
                  "commandLength": 47,
                  "sequenceNumber": 6,
                  "serviceType": "",
                  "sourceAddress": {
                    "address": "{{sourceAddr:STRING}}",
                    "ton": "{{sourceAddrTon:HEX}}",
                    "npi": "{{sourceAddrNpi:HEX}}"
                  },
                  "destAddress": {
                    "address": "{{destinationAddr:LIST}}",
                    "ton": "{{destAddrTon:INT}}",
                    "npi": "{{destAddrNpi:HEX}}"
                  },
                  "scheduleDeliveryTime": "",
                  "validityPeriod": "",
                  "priority": "0x00",
                  "registerDelivery": "{{registeredDelivery:BOOLEAN}}",
                  "replaceIfPresent": "0x00",
                  "messageLength": 6,
                  "message": "{{shortMessage:STRING}}",
                  "clientId": "{{systemId:STRING}}",
                  "host": "127.0.0.1",
                  "esmClass": "{{esmClass:HEX}}",
                  "optParams": "{{optionalParameters:LIST}}"
                }
                """;
            }
        }

        payloadMapper.setTemplate(bodyString);
        payloadMappers.add(payloadMapper);
        return payloadMappers;
    }

    private static List<PayloadMapper> getPayloadMappersDefault() {
        List<String> interpreterKeyTemplate = new ArrayList<>(Arrays.asList(
                "message|input",
                "message|output",
                "response_message|input",
                "response_message|output",
                "response_dlr|output",
                "dlr|input",
                "dlr|output"));

        String interpreterDefaultPath = """
			{
				"message|input": "/message",
				"dlr|input": "/dlr"
			}
			""";
        String interpreterDefaultTemplate = """
			{
				"message|input": {
				  "system_id": "systemId:STRING",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr}}",
				  "esm_class": "{{esmClass:INT}}",
				  "validity_period": "{{validityPeriod:LONG}}",
				  "registered_delivery": "{{registeredDelivery:INT}}",
				  "data_coding": "{{dataCoding:INT}}}",
				  "sm_default_msg_id": "{{smDefaultMsgId:INT}}",
				  "short_message": "{{shortMessage:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_parameters": "{{customParams:LIST}}"
				},
			
				"message|output": {
				  "message_id": "{{messageId:STRING}}",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr}}",
				  "registered_delivery": "{{registeredDelivery:INT}}",
				  "data_coding": "{{dataCoding:INT}}}",
				  "short_message": "{{shortMessage:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_parameters": "{{customParams:LIST}}"
				},
			
				"dlr|output": {
				  "message_id": "{{parentId:STRING}}",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr:STRING}}",
				  "esm_class": "{{esmClass:INT}}",
				  "data_coding": "{{dataCoding:INT}}",
				  "short_message": "{{shortMessage:STRING}}",
				  "status": "{{status:STRING}}",
				  "error_code": "{{errorCode:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "msg_reference_number": "{{msgReferenceNumber:STRING}}",
				  "total_segment": "{{totalSegment:INT}}",
				  "segment_sequence": "{{segmentSequence:INT}}}"
				},
			
				"dlr|input": {
				  "message_id": "{{messageId:STRING}}",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr:STRING}}",
				  "data_coding": "{{dataCoding:INT}}",
				  "status": "{{status:STRING}}",
				  "error_code": "{{errorCode:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}"
				},
			
				"response_message|input": {
				  "message_id": "{{messageId:STRING}}"
				},
			
				"response_message|output": {
				  "system_id": "{{systemId:STRING}}",
				  "message_id": "{{messageId:STRING}}",
				  "error_message": "{{errorMessage:STRING}}"
				},
			
				"response_dlr|output": {
				  "system_id": "{{systemId:STRING}}",
				  "message_id": "{{messageId:STRING}}",
				  "error_message": "{{shortMessage:STRING}}"
				}
			}
			""";

        List<PayloadMapper> interpreters = new ArrayList<>();
        JsonNode templates = Converter.stringToObject(interpreterDefaultTemplate, JsonNode.class);
        JsonNode defaultPath = Converter.stringToObject(interpreterDefaultPath, JsonNode.class);

        for (String key : interpreterKeyTemplate) {
            PayloadMapper interpreter = new PayloadMapper();

            String[] parts = key.split("\\|");
            String eventType = parts[0];
            String direction = parts[1];

            if (defaultPath.has(key)) {
                interpreter.setPath(defaultPath.get(key).asText());
            }
            interpreter.setEventType(eventType);
            interpreter.setDirection(direction);
            interpreter.setBodyType("JSON");
            interpreter.setUseProxy(false);
            interpreter.setTemplate(templates.get(key).toString());
            interpreters.add(interpreter);
        }

        return interpreters;
    }

    @ParameterizedTest(name = "High TPS: {0}, Medium TPS: {1}, Low TPS: {2} -> Creates {3} consumer(s)")
    @DisplayName("Priority consumer initialization based on TPS quotas creates correct number of consumers")
    @CsvSource({
            "70, 20, 10, 3",
            "70,  0,  0, 1",
            "70, 20,  0, 2"
    })
    void priorityConsumerInitializationWithTpsQuotasCreatesCorrectConsumers(int highTps, int mediumTps, int lowTps, int expectedConsumers) {
        when(appProperties.getKafkaBootstrapServers()).thenReturn("localhost:9092");
        when(kafkaConsumerFactory.createConsumer(any(), any())).thenReturn(kafkaCon);
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setMessagesPerSecondHigh(highTps);
        httpGW.setMessagesPerSecondMedium(mediumTps);
        httpGW.setMessagesPerSecondLow(lowTps);
        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW,
                errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        gatewayHttpConnection.connect();
        verify(kafkaConsumerFactory, times(expectedConsumers)).createConsumer(any(), any());
        gatewayHttpConnection.stopConnection();
    }

    public static ConsumerRecords<String, String> fromSingleRecord(String event) {
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("3.http.sms", 1, 1L, "key1", event);
        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        List<ConsumerRecord<String, String>> recordList = Collections.singletonList(consumerRecord);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(topicPartition, recordList);
        return new ConsumerRecords<>(recordsMap);
    }


    @Test
    @DisplayName("Build query param request for QP mode")
    void buildQueryParamRequestWhenQpModeThenUrlBuiltCorrectly() throws Exception {
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setIp("http://example.com/api?__mode=qp");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setShortMessage("Hello World");
        messageEvent.setSourceAddr("5058888888");
        messageEvent.setDestinationAddr("50577777777");

        Map<String, Object> customParams = new HashMap<>();
        customParams.put("system_id", "httpgw");
        customParams.put("pwd", "1234");
        messageEvent.setCustomParams(customParams);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        HttpRequest request = invokePrivateMethodBuildQueryParamRequest(gatewayHttpConnection, messageEvent);

        String expectedUrl = "http://example.com/api?user=httpgw&pwd=1234&senderid=5058888888&phone=50577777777&msgtext=Hello+World";
        assertEquals(expectedUrl, request.uri().toString());
        assertEquals("GET", request.method());
    }


    @Test
    @DisplayName("Build query param request for SELCOM mode")
    void buildQueryParamRequestWhenSelModeThenUrlBuiltCorrectly() throws Exception {
        Gateway httpGW = getHTTPGw("JSON");
        httpGW.setIp("http://example.com/api?__mode=sel");
        MessageEvent messageEvent = getMessageEvent();
        messageEvent.setShortMessage("Hello World");
        messageEvent.setSourceAddr("5058888888");
        messageEvent.setDestinationAddr("50577777777");

        Map<String, Object> customParams = new HashMap<>();
        customParams.put("system_id", "httpgw");
        customParams.put("pwd", "1234");
        messageEvent.setCustomParams(customParams);

        gatewayHttpConnection = new GatewayHttpConnection(appProperties, redisManager, httpGW, errorCodeMappingConcurrentHashMap, scyllaManager, kafkaTemplate, kafkaConsumerFactory);
        HttpRequest request = invokePrivateMethodBuildQueryParamRequest(gatewayHttpConnection, messageEvent);

        String expectedUrl = "http://example.com/api?USERNAME=httpgw&PASSWORD=1234&DESTADDR=50577777777&MESSAGE=Hello+World";
        assertEquals(expectedUrl, request.uri().toString());
        assertEquals("GET", request.method());
    }



    private static HttpRequest invokePrivateMethodBuildQueryParamRequest(Object targetObject, MessageEvent messageEvent)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = targetObject.getClass().getDeclaredMethod("buildQueryParamsRequest", MessageEvent.class);
        method.setAccessible(true);
        return (HttpRequest) method.invoke(targetObject, messageEvent);
    }
}
