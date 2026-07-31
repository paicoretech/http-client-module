package com.client.http.config;

import com.client.http.components.CustomFrameHandler;
import com.client.http.utils.AppProperties;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.ws.SocketClient;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static com.client.http.utils.Constants.UPDATE_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.CONNECT_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.STOP_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.DELETE_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.UPDATE_ERROR_CODE_MAPPING_ENDPOINT;

@Slf4j
@Generated
@Configuration
@RequiredArgsConstructor
public class WebSocketConfig {
    private final AppProperties appProperties;
    private final SocketSession socketSession;
    private final CustomFrameHandler customFrameHandler;

    @Bean
    public SocketClient socketClient() {
        List<String> topicsToSubscribe = List.of(
                UPDATE_GATEWAY_ENDPOINT,
                CONNECT_GATEWAY_ENDPOINT,
                STOP_GATEWAY_ENDPOINT,
                DELETE_GATEWAY_ENDPOINT,
                UPDATE_ERROR_CODE_MAPPING_ENDPOINT
        );
        UtilsRecords.WebSocketConnectionParams wsp = new UtilsRecords.WebSocketConnectionParams(
                appProperties.isWebsocketEnabled(),
                appProperties.getWebSocketHost(),
                appProperties.getWebSocketPort(),
                appProperties.getWebSocketPath(),
                topicsToSubscribe,
                appProperties.getWebsocketHeaderName(),
                appProperties.getWebsocketHeaderValue(),
                appProperties.getWebsocketRetryInterval(),
                "HTTP-CLIENT"
        );
        return new SocketClient(customFrameHandler, wsp, socketSession);
    }
}
