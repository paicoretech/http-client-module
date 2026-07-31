package com.client.http.http;

import com.client.http.utils.AppProperties;
import com.client.http.utils.InterpreterUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.ErrorCodeMapping;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaConsumerCustomImpl;
import com.paicbd.smsc.kafka.KafkaConsumerFactory;
import com.paicbd.smsc.kafka.KafkaConsumerHandler;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.paicbd.smsc.utils.ChargingUtils;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicbd.smsc.utils.Watcher;
import lombok.Getter;
import lombok.Setter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.util.DeliveryReceiptState;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static com.client.http.utils.Constants.PROXY_MODE_CUSTOM_PARAM;
import com.paicbd.smsc.utils.ErrorCodes;

import static com.client.http.utils.Constants.INPUT_DIRECTION;
import static com.client.http.utils.Constants.MESSAGE_EVENT_TYPE;
import static com.client.http.utils.Constants.OUTPUT_DIRECTION;
import static com.client.http.utils.Constants.PROXY_MODE_CUSTOM_PARAM;
import static com.client.http.utils.Constants.RESPONSE_MESSAGE_EVENT_TYPE;
import static com.client.http.utils.Constants.STOPPED;

@Slf4j
public class GatewayHttpConnection extends KafkaConsumerHandler {
    private final AtomicInteger requestCounterTotal = new AtomicInteger(0);
    private Watcher submitSmWatcher;

    @Getter
    @Setter
    private Gateway gateway;
    @Setter
    private List<PayloadMapper> interpreters;

    private final AppProperties appProperties;
    private final RedisManager redisManager;
    @Getter
    private final ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap;
    private final HttpClient httpClient;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final CdrProcessor cdrProcessor = new CdrProcessor();

    private KafkaConsumerCustomImpl kafkaConsumerHigh;
    private KafkaConsumerCustomImpl kafkaConsumerMedium;
    private KafkaConsumerCustomImpl kafkaConsumerLow;
    private ScheduledExecutorService processorScheduler;
    private ScheduledFuture<?> processorTask;
    private ExecutorService sendingExecutor;

    private volatile boolean stopped = true;

    @SuppressWarnings("java:S107")
    public GatewayHttpConnection(AppProperties appProperties, RedisManager redisManager, Gateway gateway,
                                 ConcurrentMap<String, List<ErrorCodeMapping>> errorCodeMappingConcurrentHashMap,
                                 ScyllaManager scyllaManager, KafkaTemplate<String, String> kafkaTemplate, KafkaConsumerFactory kafkaConsumerFactory) {
        this.appProperties = appProperties;
        this.redisManager = redisManager;
        this.gateway = gateway;
        this.errorCodeMappingConcurrentHashMap = errorCodeMappingConcurrentHashMap;
        this.scyllaManager = scyllaManager;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.interpreters = gateway.getInterpreter();
        this.httpClient = appProperties.isHttp2() ? HttpClient.newHttpClient() : HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }

    private void initializeConsumers() {
        int networkId = this.gateway.getNetworkId();
        int mpsHigh = gateway.getMessagesPerSecondHigh();
        int mpsMedium = gateway.getMessagesPerSecondMedium();
        int mpsLow = gateway.getMessagesPerSecondLow();

        if (mpsHigh > 0) {
            String topicHigh = networkId + KafkaTopicsConstants.HTTP_HIGH_MESSAGE_TOPIC_SUFFIX;
            String groupIdHigh = KafkaConsumerConstants.HTTP_HIGH_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerHigh = new KafkaConsumerCustomImpl(topicHigh, groupIdHigh,
                    appProperties.getKafkaBootstrapServers(), gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory, scyllaManager);
        }
        if (mpsMedium > 0) {
            String topicMedium = networkId + KafkaTopicsConstants.HTTP_MEDIUM_MESSAGE_TOPIC_SUFFIX;
            String groupIdMedium = KafkaConsumerConstants.HTTP_MEDIUM_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerMedium = new KafkaConsumerCustomImpl(topicMedium, groupIdMedium,
                    appProperties.getKafkaBootstrapServers(), gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory, scyllaManager);
        }
        if (mpsLow > 0) {
            String topicLow = networkId + KafkaTopicsConstants.HTTP_LOW_MESSAGE_TOPIC_SUFFIX;
            String groupIdLow = KafkaConsumerConstants.HTTP_LOW_MESSAGE_GROUP_ID_PREFIX + networkId;
            kafkaConsumerLow = new KafkaConsumerCustomImpl(topicLow, groupIdLow,
                    appProperties.getKafkaBootstrapServers(), gateway.getMessagesPerSecond(),
                    kafkaConsumerFactory, scyllaManager);
        }
    }

    public void connect() {
        log.info("Connected to Gateway {}", this.gateway.getName());
        initializeConsumers();
        this.stopped = false;
        this.submitSmWatcher = new Watcher(
                "HttpClient:OutboundSubmitSm-NetworkId-" + this.gateway.getNetworkId() + "-",
                this.requestCounterTotal, 1);
        this.gateway.setStatus("STARTED");
        this.gateway.setEnabled(1);
        redisManager.hset(GeneralSmscConstants.SMPP_HTTP_GATEWAYS_HASH_NAME, String.valueOf(gateway.getNetworkId()), this.gateway.toString());
        checkAndManageConsumerState();
    }

    public void stopConnection() {
        this.stopped = true;
        gateway.setStatus(STOPPED);
        gateway.setSuccessSession(0);
        checkAndManageConsumerState();
        if (Objects.nonNull(this.submitSmWatcher)) {
            this.submitSmWatcher.stopWatching();
        }
    }

    @Synchronized
    private void checkAndManageConsumerState() {
        boolean isActuallyRunning = (processorTask != null && !processorTask.isCancelled());

        if (!this.stopped && !isActuallyRunning) {
            log.info("Gateway active. Starting priority kafka consumers.");
            startConsumer();
        } else if (this.stopped && isActuallyRunning) {
            log.info("Gateway stopped. Stopping priority kafka consumers.");
            stopConsumer();
        }
    }

    private void startConsumer() {
        startSenderResources();
        startConsumers();
        startProcessor();
    }

    private void startSenderResources() {
        if (Objects.isNull(this.sendingExecutor)) {
            ThreadFactory senderFactory = Thread.ofVirtual()
                    .name("SenderPool-NetworkId-" + this.gateway.getNetworkId() + "-", 1)
                    .factory();
            this.sendingExecutor = Executors.newThreadPerTaskExecutor(senderFactory);
        }
    }

    private void startConsumers() {
        if (Objects.nonNull(this.kafkaConsumerHigh)) this.kafkaConsumerHigh.startConsumer();
        if (Objects.nonNull(this.kafkaConsumerMedium)) this.kafkaConsumerMedium.startConsumer();
        if (Objects.nonNull(this.kafkaConsumerLow)) this.kafkaConsumerLow.startConsumer();
    }

    private void startProcessor() {
        if (Objects.isNull(this.processorScheduler)) {
            this.processorScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r);
                t.setName("Processor-Scheduler-NetworkId-" + this.gateway.getNetworkId());
                t.setDaemon(true);
                return t;
            });
        }
        processorTask = this.processorScheduler.scheduleAtFixedRate(() -> {
            try {
                if (this.stopped) return;
                this.processMessages();
            } catch (Exception e) {
                log.error("Error in HTTP message processor: {}", e.getMessage(), e);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void processMessages() {
        int mpsHigh = gateway.getMessagesPerSecondHigh();
        int mpsMedium = gateway.getMessagesPerSecondMedium();
        int mpsLow = gateway.getMessagesPerSecondLow();

        var messages = this.fetchMessages(
                gateway.getMessagesPerSecond(), mpsHigh, mpsMedium, mpsLow,
                kafkaConsumerHigh, kafkaConsumerMedium, kafkaConsumerLow);

        if (messages.isEmpty()) return;
        this.sendSubmitSmList(messages);
    }

    private void stopConsumer() {
        stopProcessor();
        stopConsumers();
        stopSenderResources();
    }

    private void stopSenderResources() {
        if (Objects.nonNull(this.sendingExecutor)) {
            this.sendingExecutor.shutdownNow();
            this.sendingExecutor = null;
        }
    }

    private void stopProcessor() {
        if (Objects.nonNull(this.processorTask)) {
            this.processorTask.cancel(true);
            this.processorTask = null;
        }
        if (Objects.nonNull(this.processorScheduler)) {
            this.processorScheduler.shutdownNow();
            this.processorScheduler = null;
        }
    }

    private void stopConsumers() {
        if (Objects.nonNull(this.kafkaConsumerHigh)) this.kafkaConsumerHigh.stopConsumer();
        if (Objects.nonNull(this.kafkaConsumerMedium)) this.kafkaConsumerMedium.stopConsumer();
        if (Objects.nonNull(this.kafkaConsumerLow)) this.kafkaConsumerLow.stopConsumer();
        this.kafkaConsumerHigh = null;
        this.kafkaConsumerMedium = null;
        this.kafkaConsumerLow = null;
    }

    private HttpRequest newRequest(String body, String contentType, Gateway gw, MessageEvent submitSmEvent) {
        HttpRequest.Builder httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(this.gateway.getIp()))
                .header("Content-Type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .timeout(Duration.ofMillis(this.gateway.getPduTimeout()));

        if (Objects.nonNull(gw.getAuthenticationTypes()) && !"Undefined".equalsIgnoreCase(gw.getAuthenticationTypes())) {
            httpRequest.header(gw.getHeaderSecurityName(), gw.getToken());
        }

        InterpreterUtils.forEachInterpreterHeader(
                gw, submitSmEvent, OUTPUT_DIRECTION, MESSAGE_EVENT_TYPE,
                httpRequest::header
        );

        return httpRequest.build();
    }

    private void sendToRetryProcess(MessageEvent submitSmEventToRetry, int errorCode) {
        log.warn("Starting retry process for submit_sm with id {} and error code {}", submitSmEventToRetry.getMessageId(), errorCode);
        if (errorContained(gateway.getNoRetryErrorCode(), errorCode)) {
            this.handlerFailedMessage(submitSmEventToRetry, errorCode);
            return;
        }

        if (errorContained(gateway.getRetryAlternateDestinationErrorCode(), errorCode)) {
            this.handleRetryAlternateDestination(submitSmEventToRetry);
            return;
        }

        this.handleAutoRetry(submitSmEventToRetry, errorCode);
    }

    private void handleRetryAlternateDestination(MessageEvent originalSubmitSmEventToRetry) {
        this.cdrProcessor.publishCdr(originalSubmitSmEventToRetry, UtilsEnum.Module.HTTP_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
        MessageEvent submitSmEventToRetry = Converter.deepCopy(originalSubmitSmEventToRetry, MessageEvent.class);
        this.prepareForRetry(submitSmEventToRetry);
        String kafkaTopic = KafkaUtils.getRoutingTopicPriority(submitSmEventToRetry.getSmscMessagePriority());
        log.debug("Retry for submit_sm with id {}. new networkId {}", submitSmEventToRetry.getMessageId(), submitSmEventToRetry.getDestNetworkId());
        this.kafkaTemplate.send(kafkaTopic, submitSmEventToRetry.toString());
    }


    private void handleAutoRetry(MessageEvent submitSmEventToRetry, int errorCode) {
        log.info("AutoRetryErrorCodes defined {}, ReceivedErrorCode {}", gateway.getAutoRetryErrorCode(), errorCode);
        if (errorContained(gateway.getAutoRetryErrorCode(), errorCode)) {
            log.info("AutoRetrying for submit_sm with id {}", submitSmEventToRetry.getMessageId());
            this.cdrProcessor.publishCdr(submitSmEventToRetry, UtilsEnum.Module.HTTP_CLIENT,
                    UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
            this.prepareForRetry(submitSmEventToRetry);
            String topicRetries = KafkaUtils.getRetriesTopicPriority(submitSmEventToRetry.getSmscMessagePriority());
            kafkaTemplate.send(topicRetries, submitSmEventToRetry.toString());
            log.info("Successfully added to Kafka topic {} -> {}", topicRetries, submitSmEventToRetry);
            return;
        }

        log.warn("Failed to retry for submit_sm with id {}. The gateway {} doesn't have the error code {} for the retry process.", submitSmEventToRetry.getMessageId(), this.getGateway().getName(), errorCode);
        this.handlerFailedMessage(submitSmEventToRetry, errorCode);
    }

    private void prepareForRetry(MessageEvent submitSmEventToRetry) {
        submitSmEventToRetry.setErrorCode(null);
        submitSmEventToRetry.setRetry(true);
        submitSmEventToRetry.setRetryNumber(Objects.isNull(submitSmEventToRetry.getRetryNumber()) ? 1 : submitSmEventToRetry.getRetryNumber() + 1);
    }

    private void handlerFailedMessage(MessageEvent submitSmEvent, int errorCode) {
        this.cdrProcessor.publishCdr(submitSmEvent, UtilsEnum.Module.HTTP_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
        this.sendDeliverSmForFailedCases(submitSmEvent, errorCode);
        if (ChargingUtils.checkMessageForRefund(submitSmEvent)) {
            log.debug("Applying refund for message with id {}", submitSmEvent.getMessageId());
            String topicByPriority = KafkaUtils.getChargingTopicPriority(submitSmEvent.getSmscMessagePriority());
            this.kafkaTemplate.send(topicByPriority, submitSmEvent.toString());
        }
    }

    private boolean errorContained(String stringList, int errorCode) {
        return Arrays.stream(stringList.split(","))
                .map(String::trim)
                .anyMatch(code -> code.equals(String.valueOf(errorCode)));
    }

    private void sendDeliverSmForFailedCases(MessageEvent submitSmEventToRetry, int errorCode) {
        if (submitSmEventToRetry.getRegisteredDelivery() == RequestDelivery.REQUEST_DLR.getValue()) {
            if (!submitSmEventToRetry.isFinalSegmentForSplitSmsc()) return;
            ErrorCodeMapping errorCodeMapping = getErrorCodeMapping(errorCode);
            MessageEvent deliverSmEvent = submitSmEventToRetry.createDeliveryReceiptMessage(errorCodeMapping, null);
            this.kafkaTemplate.send(KafkaTopicsConstants.PRE_DELIVER_TOPIC, deliverSmEvent.toString());
        }
    }

    private ErrorCodeMapping getErrorCodeMapping(Integer errorCode) {
        List<ErrorCodeMapping> errorCodeMappingList = errorCodeMappingConcurrentHashMap.get(String.valueOf(gateway.getMno()));
        return Optional.ofNullable(errorCodeMappingList)
                .flatMap(list -> list.stream().filter(errorCodeMapping -> errorCodeMapping.getErrorCode() == errorCode).findFirst())
                .orElseGet(() -> {
                    log.debug("No error code mapping found for mno {} with error {}. using status {}",
                            gateway.getMno(), errorCode, DeliveryReceiptState.UNDELIV);
                    ErrorCodeMapping defaultMapping = new ErrorCodeMapping();
                    defaultMapping.setErrorCode(errorCode);
                    defaultMapping.setDeliveryErrorCode(errorCode);
                    defaultMapping.setDeliveryStatus("UNDELIV");
                    return defaultMapping;
                });
    }

    private void sendSubmitSmList(List<MessageEvent> events) {
        for (MessageEvent submitSmEvent : events) {
            CompletableFuture.runAsync(() -> {
                try {
                    if (submitSmEvent.getMessageParts() != null) {
                        submitSmEvent.getMessageParts().forEach(msgPart -> {
                            var messageEvent = Converter.deepCopy(submitSmEvent, MessageEvent.class);
                            messageEvent.setMessageId(msgPart.getMessageId());
                            messageEvent.setShortMessage(msgPart.getShortMessage());
                            messageEvent.setMsgReferenceNumber(msgPart.getMsgReferenceNumber());
                            messageEvent.setTotalSegment(msgPart.getTotalSegment());
                            messageEvent.setSegmentSequence(msgPart.getSegmentSequence());
                            messageEvent.setOptionalParameters(msgPart.getOptionalParameters());
                            messageEvent.setMessageBytes(msgPart.getPartBytes());
                            messageEvent.setUdhBytes(msgPart.getUdhBytes());
                            messageEvent.setUdhRaw(msgPart.getUdhRaw());
                            messageEvent.setMessageParts(null);
                            messageEvent.addCustomParam(msgPart.getCustomParams());
                            sendMessage(messageEvent);
                        });
                    } else {
                        sendMessage(submitSmEvent);
                    }
                } catch (Exception e) {
                    log.error("Failed to send Message list", e);
                }
            }, sendingExecutor);
        }
    }

    private static final String QUERY_PARAMS_FLAG = "__mode=qp";
    private static final String SELCOM_PARAMS_FLAG = "__mode=sel";

    private boolean isQueryParamsMode() {
        return Objects.nonNull(this.gateway.getIp())
                && this.gateway.getIp().contains(QUERY_PARAMS_FLAG);
    }

    private boolean isSelcomMode() {
        return Objects.nonNull(this.gateway.getIp())
                && this.gateway.getIp().contains(SELCOM_PARAMS_FLAG);
    }

    private String cleanGatewayUrl() {
        String ip = this.gateway.getIp();
        return ip.replace("?" + QUERY_PARAMS_FLAG, "")
                .replace("&" + QUERY_PARAMS_FLAG, "")
                .replace(QUERY_PARAMS_FLAG, "")
                .replace("?" + SELCOM_PARAMS_FLAG, "")
                .replace("&" + SELCOM_PARAMS_FLAG, "")
                .replace(SELCOM_PARAMS_FLAG, "");
    }

    private HttpRequest buildQueryParamsRequest(MessageEvent submitSmEvent) {
        Map<String, Object> customParams = submitSmEvent.getCustomParams();
        String user = customParams != null ? String.valueOf(customParams.getOrDefault("system_id", "")) : "";
        String pwd = customParams != null ? String.valueOf(customParams.getOrDefault("pwd", "")) : "";
        String senderId = Objects.toString(submitSmEvent.getSourceAddr(), "");
        String phone = Objects.toString(submitSmEvent.getDestinationAddr(), "");
        String msgText = Objects.toString(submitSmEvent.getShortMessage(), "");
        String url;
        if (isSelcomMode()) {
            url = cleanGatewayUrl() + "?"
                    + "USERNAME=" + URLEncoder.encode(user, StandardCharsets.UTF_8)
                    + "&PASSWORD=" + URLEncoder.encode(pwd, StandardCharsets.UTF_8)
                    + "&DESTADDR=" + URLEncoder.encode(phone, StandardCharsets.UTF_8)
                    + "&MESSAGE=" + URLEncoder.encode(msgText, StandardCharsets.UTF_8);
        } else {
            url = cleanGatewayUrl() + "?"
                    + "user=" + URLEncoder.encode(user, StandardCharsets.UTF_8)
                    + "&pwd=" + URLEncoder.encode(pwd, StandardCharsets.UTF_8)
                    + "&senderid=" + URLEncoder.encode(senderId, StandardCharsets.UTF_8)
                    + "&phone=" + URLEncoder.encode(phone, StandardCharsets.UTF_8)
                    + "&msgtext=" + URLEncoder.encode(msgText, StandardCharsets.UTF_8);
        }

        log.debug("QueryParams request to: {}", cleanGatewayUrl());

        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .GET()
                .timeout(Duration.ofMillis(this.gateway.getPduTimeout()))
                .build();
    }


    private void sendMessage(MessageEvent submitSmEvent) {
        HttpRequest request;
        if (isQueryParamsMode() || isSelcomMode()) {
            request = buildQueryParamsRequest(submitSmEvent);
        } else {
            String contentType = "application/json";

            // interpreter mapper
            PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(this.interpreters, MESSAGE_EVENT_TYPE, OUTPUT_DIRECTION);
            String requestBody = InterpreterUtils.interpreterFormatForMT(submitSmEvent, payloadMapper);
            if (Objects.nonNull(payloadMapper)) {
                contentType = "XML".equalsIgnoreCase(payloadMapper.getBodyType()) ? "application/xml" : contentType;
            }

            // make the request
            Objects.requireNonNull(requestBody);
            request = newRequest(requestBody, contentType, gateway, submitSmEvent);
        }

        // sending request
        try {
            // Send the HTTP Request and wait the response
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            requestCounterTotal.incrementAndGet();
            if (response.statusCode() == 200) {
                String gatewayMessageId = addInCache(submitSmEvent, response);
                sendProxyResponse(submitSmEvent, UtilsEnum.CdrStatus.SUCCESS, gatewayMessageId);
            } else {
                this.globalErrorHandler(submitSmEvent, response.statusCode());
            }
        } catch (HttpTimeoutException e) {
            log.error("An exception occurred trying to send http request due to HttpTimeoutException -> {}", e.getMessage(), e);
            this.globalErrorHandler(submitSmEvent, 408);
        } catch (ConnectException e) {
            log.error("An exception occurred trying to send http request due to ConnectException -> {}", e.getMessage(), e);
            this.globalErrorHandler(submitSmEvent, 503);
        } catch (IOException | InterruptedException e) {
            log.error("An exception occurred trying to send http request -> {}", e.getMessage(), e);
            this.globalErrorHandler(submitSmEvent, 500);
            Thread.currentThread().interrupt();
        }
    }

    private void globalErrorHandler(MessageEvent submitSmEvent, int errorCode) {
        submitSmEvent.setErrorCode(errorCode);
        // Proxy mode: report the result of this delivery attempt to the waiting origin module,
        // mirroring smpp-client-module behavior (the retry process continues independently).
        // No gateway message id on failure: the origin keeps its internal id for the error response.
        sendProxyResponse(submitSmEvent, UtilsEnum.CdrStatus.FAILED, null);
        if (submitSmEvent.getValidityPeriod() == 0) {
            log.debug("The validity period for submit_sm with id {} is 0, the message will be sent to DLR", submitSmEvent.getMessageId());
            this.handlerFailedMessage(submitSmEvent, errorCode);
            return;
        }

        if (submitSmEvent.isLastRetry()) {
            log.debug("Last retry for submit_sm with id {}, the message will be sent to DLR", submitSmEvent.getMessageId());
            this.handlerFailedMessage(submitSmEvent, errorCode);
            return;
        }
        this.sendToRetryProcess(submitSmEvent, errorCode);
    }


    private String addInCache(MessageEvent submitSmEvent, HttpResponse<String> response) {
        MessageEvent responseEvent = new MessageEvent();
        PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(this.interpreters, RESPONSE_MESSAGE_EVENT_TYPE, INPUT_DIRECTION);
        InterpreterUtils.interpreterFormatForMO(responseEvent, response.body(), payloadMapper);
        // The downstream gateway-assigned id from the HTTP 200 body. It is the CDR's mno_message_id and,
        // in proxy mode, the id returned to the ESME in submit_sm_resp. Independent of whether a DLR
        // was requested, so the CDR carries it even when no DLR cache entry is created below.
        String gatewayMessageId = responseEvent.getMessageId();
        String hashKey = "";
        if (submitSmEvent.getRegisteredDelivery() == RequestDelivery.REQUEST_DLR.getValue()) {
            if (Objects.nonNull(gatewayMessageId) && !gatewayMessageId.isBlank()) {
                log.debug("Requesting DLR for submit_sm with id {} and messageId {}", submitSmEvent.getId(), gatewayMessageId);
                if (submitSmEvent.isUseProxy()) {
                    // Persist the proxy flag with the submit result so the DLR callback flow can
                    // hold its HTTP response until the deliver_sm is acknowledged by the ESME.
                    submitSmEvent.addCustomParam(PROXY_MODE_CUSTOM_PARAM, true);
                }
                UtilsRecords.SubmitSmResponseEvent submitSmResponseEvent = new UtilsRecords.SubmitSmResponseEvent(
                        gatewayMessageId,
                        System.currentTimeMillis() + "-" + System.nanoTime(),
                        submitSmEvent.getSystemId(),
                        gatewayMessageId,
                        submitSmEvent.getMessageId(),
                        submitSmEvent.getOriginProtocol().toUpperCase(),
                        submitSmEvent.getOriginNetworkId(),
                        submitSmEvent.getOriginNetworkType(),
                        submitSmEvent.getMsgReferenceNumber(),
                        submitSmEvent.getTotalSegment(),
                        submitSmEvent.getSegmentSequence(),
                        submitSmEvent.getParentId(),
                        submitSmEvent.getDestNetworkId(),
                        submitSmEvent.isApplyForRefund(),
                        submitSmEvent.getCustomParams(),
                        submitSmEvent.isSplitForSmsc(),
                        submitSmEvent.getSourceUri(),
                        submitSmEvent.getDestinationUri(),
                        submitSmEvent.getSmscMessagePriority()
                );
                hashKey = Objects.nonNull(submitSmResponseEvent.hashId()) ? submitSmResponseEvent.hashId().toUpperCase() : null;
                this.scyllaManager.insertIntoTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE,
                        hashKey, submitSmResponseEvent.toString());
            } else {
                log.warn("The response body doesn't contains message_id {} ", response);
            }
        }
        String cdrComment = null;
        if (isQueryParamsMode() || isSelcomMode()) {
            JsonNode node = Converter.stringToObject(response.body(), JsonNode.class);
            cdrComment = (node != null) ? node.toString() : response.body();
        }
        // mno_message_id is the gateway-assigned id (the submit_sm_resp UUID), not the Scylla hash key.
        cdrProcessor.publishCdr(submitSmEvent, gatewayMessageId, UtilsEnum.Module.HTTP_CLIENT,
                UtilsEnum.MessageType.MESSAGE, UtilsEnum.CdrStatus.SUCCESS, cdrComment, kafkaTemplate);
        // Surfaced to proxy mode so the origin's submit_sm_resp carries the gateway-assigned id
        return gatewayMessageId;
    }

    public void publishInKafkaTopic(MessageEvent messageEvent) {
        String payload = messageEvent.toString();
        String kafkaTopic = "";
        try {
            if (!messageEvent.isDlr()) {
                kafkaTopic = KafkaUtils.getMessageDestinationTopic(messageEvent);
            } else if (messageEvent.isProcess()) {
                kafkaTopic = KafkaUtils.getDeliveryDestinationTopic(messageEvent);
            }
            kafkaTemplate.send(kafkaTopic, payload);
        } catch (Exception ex) {
            log.error("Error on try to publish on kafka", ex);
        }
    }

    private void sendProxyResponse(MessageEvent event, UtilsEnum.CdrStatus cdrStatus, String gatewayMessageId) {
        if (event.isUseProxy()) {
            boolean completedWithError = false;
            String errorMsg = "";
            if (cdrStatus.equals(UtilsEnum.CdrStatus.FAILED)) {
                completedWithError = true;
                int errorCode = Objects.nonNull(event.getErrorCode()) ? event.getErrorCode() : 0;
                errorMsg = ErrorCodes.getErrorDescription(UtilsEnum.Module.HTTP_CLIENT, errorCode, null);
            }
            // messageId remains the correlation key (the origin's internal id); gatewayMessageId carries
            // the downstream-assigned id that proxy mode returns to the ESME in submit_sm_resp.
            UtilsRecords.HttpProxyResponse httpProxyResponse = new UtilsRecords.HttpProxyResponse(
                    event.getMessageId(), completedWithError, event.getErrorCode(), errorMsg, gatewayMessageId);

            //Get the HTTP Response and sent it to Kafka (HTTP Proxy Topic)
            //Find the Next Step in SMSC-Routing-Module (CommonProcessor.processRoutingAction()
            kafkaTemplate.send(KafkaTopicsConstants.HTTP_PROXY_TOPIC, httpProxyResponse.toString());
        }
    }
}
