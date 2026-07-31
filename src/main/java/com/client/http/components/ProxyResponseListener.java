package com.client.http.components;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProxyResponseListener {
    private final ProxyResponseHandler proxyResponseHandler;

    // Module-specific consumer group: both proxy listeners (this module and smpp-server) share the topic
    @KafkaListener(topics = KafkaTopicsConstants.HTTP_PROXY_TOPIC, groupId = KafkaConsumerConstants.HTTP_PROXY_GROUP_ID_PREFIX + ".http-client", concurrency = "1")
    public void handleDeliverySms(List<String> messages) {
        if (Objects.isNull(messages) || messages.isEmpty()) {
            return;
        }

        for (String message : messages) {
            UtilsRecords.HttpProxyResponse proxyResult = Converter.stringToObject(message, UtilsRecords.HttpProxyResponse.class);
            if (Objects.isNull(proxyResult) || Objects.isNull(proxyResult.messageId())) {
                log.warn("Discarding invalid proxy response: {}", message);
                continue;
            }
            log.debug("Received proxy response for message id {}", proxyResult.messageId());
            proxyResponseHandler.completeFuture(proxyResult.messageId(), proxyResult);
        }
    }
}
