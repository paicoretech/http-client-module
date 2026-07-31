package com.client.http.components;

import com.client.http.http.HttpClientManager;
import com.paicbd.smsc.ws.FrameHandler;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static com.client.http.utils.Constants.CONNECT_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.DELETE_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.RESPONSE_SMPP_CLIENT_ENDPOINT;
import static com.client.http.utils.Constants.STOP_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.UPDATE_ERROR_CODE_MAPPING_ENDPOINT;
import static com.client.http.utils.Constants.UPDATE_GATEWAY_ENDPOINT;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomFrameHandler implements FrameHandler  {
    private final SocketSession socketSession;
    private final HttpClientManager httpClientManager;

    @Override
    public void handleFrameLogic(StompHeaders headers, Object payload) {
        String key = payload.toString();
        try {
            Integer.parseInt(key);
            String destination = headers.getDestination();
            Objects.requireNonNull(key, "The payload data cannot be null");
            Objects.requireNonNull(destination, "Destination cannot be null");

            switch (destination) {
                case UPDATE_GATEWAY_ENDPOINT -> handleUpdate(key, this::handleUpdateGateway);
                case CONNECT_GATEWAY_ENDPOINT -> handleUpdate(key, this::handleConnectGateway);
                case STOP_GATEWAY_ENDPOINT -> handleUpdate(key, this::handleStopGateway);
                case DELETE_GATEWAY_ENDPOINT -> handleUpdate(key, this::handleDeleteGateway);
                case UPDATE_ERROR_CODE_MAPPING_ENDPOINT -> handleUpdate(key, this::handleUpdateErrorCodeMapping);
                default -> log.warn("Unknown destination: {}", destination);
            }
        } catch (NumberFormatException ex) {
            log.error("The requested payload does not contains a valid Network ID , provided value : {} ", key);
        }
    }

    private void handleUpdate(String payload, java.util.function.Consumer<String> handler) {
        handler.accept(payload);
        socketSession.getStompSession().send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    private void handleUpdateGateway(String stringNetworkId) {
        log.info("Updating gateway {}", stringNetworkId);
        httpClientManager.updateGateway(stringNetworkId);
    }

    private void handleConnectGateway(String stringNetworkId) {
        log.info("Connecting gateway {}", stringNetworkId);
        httpClientManager.connectGateway(stringNetworkId);
    }

    private void handleStopGateway(String stringNetworkId) {
        log.info("Stopping gateway {}", stringNetworkId);
        httpClientManager.stopGateway(stringNetworkId);
    }

    private void handleDeleteGateway(String stringNetworkId) {
        log.info("Deleting gateway {}", stringNetworkId);
        httpClientManager.deleteGateway(stringNetworkId);
    }

    private void handleUpdateErrorCodeMapping(String systemId) {
        log.info("Updating error code mapping for mno_id {}", systemId);
        httpClientManager.updateErrorCodeMapping(systemId);
    }
}
