package com.client.http.components;

import com.client.http.http.HttpClientManager;
import com.paicbd.smsc.ws.SocketSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;

import static com.client.http.utils.Constants.CONNECT_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.DELETE_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.RESPONSE_SMPP_CLIENT_ENDPOINT;
import static com.client.http.utils.Constants.STOP_GATEWAY_ENDPOINT;
import static com.client.http.utils.Constants.UPDATE_ERROR_CODE_MAPPING_ENDPOINT;
import static com.client.http.utils.Constants.UPDATE_GATEWAY_ENDPOINT;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CustomFrameHandlerTest {

    private static final int PAYLOAD_NETWORK_ID = 154;
    private static final int PAYLOAD_MNO_ID = 256;

    @Mock
    private SocketSession mockSocketSession;

    @Mock
    private HttpClientManager mockHttpClientManager;

    @Mock
    private StompHeaders mockStompHeaders;

    @Mock
    private StompSession mockStompSession;

    @InjectMocks
    private CustomFrameHandler customFrameHandler;

    @Test
    @DisplayName("Handle frame logic test when updating a gateway")
    void handleFrameLogicWhenUpdateGatewayThenSendResponse() {
        when(mockSocketSession.getStompSession()).thenReturn(mockStompSession);
        when(mockStompHeaders.getDestination()).thenReturn(UPDATE_GATEWAY_ENDPOINT);
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_NETWORK_ID);
        verify(mockHttpClientManager).updateGateway(String.valueOf(PAYLOAD_NETWORK_ID));
        verify(mockStompSession).send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    @Test
    @DisplayName("Testing the handle frame logic when a new gateway connection is requested")
    void handleFrameLogicWhenConnectGatewayThenSendResponse() {
        when(mockSocketSession.getStompSession()).thenReturn(mockStompSession);
        when(mockStompHeaders.getDestination()).thenReturn(CONNECT_GATEWAY_ENDPOINT);
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_NETWORK_ID);
        verify(mockHttpClientManager).connectGateway(String.valueOf(PAYLOAD_NETWORK_ID));
        verify(mockStompSession).send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    @Test
    @DisplayName("Test for handle frame logic when a gateway is stopped")
    void handleFrameLogicWhenStopGatewayThenSendResponse() {
        when(mockSocketSession.getStompSession()).thenReturn(mockStompSession);
        when(mockStompHeaders.getDestination()).thenReturn(STOP_GATEWAY_ENDPOINT);
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_NETWORK_ID);
        verify(mockHttpClientManager).stopGateway(String.valueOf(PAYLOAD_NETWORK_ID));
        verify(mockStompSession).send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    @Test
    @DisplayName("Handle frame logic test for a gateway deletion event")
    void handleFrameLogicWhenDeleteGatewayThenSendResponse() {
        when(mockSocketSession.getStompSession()).thenReturn(mockStompSession);
        when(mockStompHeaders.getDestination()).thenReturn(DELETE_GATEWAY_ENDPOINT);
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_NETWORK_ID);
        verify(mockHttpClientManager).deleteGateway(String.valueOf(PAYLOAD_NETWORK_ID));
        verify(mockStompSession).send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    @Test
    @DisplayName("Testing the handle frame logic when any error code needs to be updated")
    void handleFrameLogicWhenUpdateErrorCodeThenSendResponse() {
        when(mockSocketSession.getStompSession()).thenReturn(mockStompSession);
        when(mockStompHeaders.getDestination()).thenReturn(UPDATE_ERROR_CODE_MAPPING_ENDPOINT);
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_MNO_ID);
        verify(mockHttpClientManager).updateErrorCodeMapping(String.valueOf(PAYLOAD_MNO_ID));
        verify(mockStompSession).send(RESPONSE_SMPP_CLIENT_ENDPOINT, "OK");
    }

    @Test
    @DisplayName("Verifies no sessions and client manager interactions occurs for unknown destination")
    void handleFrameLogicWhenUnknownDestinationThenDontExecuteAction() {
        when(mockStompHeaders.getDestination()).thenReturn("unknown_destination");
        customFrameHandler.handleFrameLogic(mockStompHeaders, PAYLOAD_NETWORK_ID);
        verifyNoInteractions(mockHttpClientManager);
        verifyNoInteractions(mockStompSession);
    }

    @Test
    @DisplayName("Verifies no sessions and client manager interactions occurs for invalid payloads")
    void handleFrameLogicWhenPayloadIsWrongThenDontExecuteAction() {
        customFrameHandler.handleFrameLogic(mockStompHeaders, "my_wrong_payload");
        verifyNoInteractions(mockHttpClientManager);
        verifyNoInteractions(mockStompSession);
    }
}
