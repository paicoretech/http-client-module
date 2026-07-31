package com.client.http.service;

import com.client.http.components.ProxyResponseHandler;
import com.client.http.dto.InterpreterByPathRequest;
import com.client.http.exception.NoInterpreterFoundException;
import com.client.http.exception.SmsDeliveryException;
import com.client.http.exception.SmsProcessingException;
import com.client.http.http.GatewayHttpConnection;
import com.client.http.utils.AppProperties;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.scylla.ScyllaTablesConstants;
import com.client.http.utils.InterpreterUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.paicbd.smsc.dto.Gateway;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.utils.Converter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.client.http.utils.Constants.INPUT_DIRECTION;
import static com.client.http.utils.Constants.MESSAGE_EVENT_TYPE;
import static com.client.http.utils.Constants.OUTPUT_DIRECTION;
import static com.client.http.utils.Constants.RESPONSE_DLR_EVENT_TYPE;
import static com.client.http.utils.Constants.RESPONSE_MESSAGE_EVENT_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageEventServiceTest {
    @Mock
    private ProcessMessageDlrService processMessageDlrService;

    @Mock
    private ProcessMessageService processMessageService;

    @InjectMocks
    private MessageEventService messageEventService;

    @Mock
    private AppProperties appProperties;

	@Mock
	GatewayHttpConnection gatewayHttpConnection;

    @Mock
    ScyllaManager scyllaManager;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    String systemId = "http_gw_01";
	ConcurrentMap<String, Gateway> gatewayConcurrentHashMap = new ConcurrentHashMap<>();
	ConcurrentMap<String, InterpreterByPathRequest> interpreterByPathRequestConcurrentHashMap = new ConcurrentHashMap<>();
	ConcurrentMap<String, GatewayHttpConnection> httpConnectionManagerList = new ConcurrentHashMap<>();
	ProxyResponseHandler proxyResponseHandler = new ProxyResponseHandler();

    @BeforeEach
    void setUp() {
		this.gatewayConcurrentHashMap.put(systemId, this.getGatewayWithInterpreter());
		this.interpreterByPathRequestConcurrentHashMap.put("message/message", this.createInterpreterForMOMessage());
		this.interpreterByPathRequestConcurrentHashMap.put("dlr/message", this.createInterpreterForDLRMessage());
		this.httpConnectionManagerList.put("1", gatewayHttpConnection);

        processMessageDlrService = new ProcessMessageDlrService(interpreterByPathRequestConcurrentHashMap, gatewayConcurrentHashMap, httpConnectionManagerList, scyllaManager, kafkaTemplate, proxyResponseHandler, appProperties);
        processMessageService = new ProcessMessageService(appProperties, gatewayConcurrentHashMap, interpreterByPathRequestConcurrentHashMap, kafkaTemplate, proxyResponseHandler);
        this.messageEventService = new MessageEventService(processMessageDlrService, processMessageService);
    }

	@ParameterizedTest
	@SuppressWarnings("unchecked")
    @DisplayName("Process MO message with default and custom template")
	@ValueSource(booleans = {true, false})
    void processMOMessageWithDefaultAndCustomTemplateThenDoSuccessfully(boolean useProxy) {
		if (useProxy) {
			when(appProperties.getProxyModeResponseTimeout()).thenReturn(5L);
		}

		Gateway gateway = gatewayConcurrentHashMap.get(systemId);
		List<PayloadMapper> interpreter = gateway.getInterpreter();
		PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), MESSAGE_EVENT_TYPE, INPUT_DIRECTION);
		interpreter.remove(payloadMapper);
        assert payloadMapper != null;
        payloadMapper.setUseProxy(useProxy);
		interpreter.add(payloadMapper);
		this.gatewayConcurrentHashMap.get(systemId).setInterpreter(interpreter);

		Map<String, Object> request = this.generateMoMessage(useProxy);

		if (useProxy) {
			assertThrows(SmsDeliveryException.class, () -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		} else {
			Map<String, Object> response = messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE);
			assertNotNull(response);
			assertTrue(response.containsKey("body"));
			Map<String, String> body = (Map<String, String>) response.get("body");
			assertTrue(body.containsKey("message_id") && Objects.nonNull(body.get("message_id")));
		}

		verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC), anyString());
    }

	@ParameterizedTest
	@DisplayName("Process MO message when gateway is null or disabled then message is received")
	@ValueSource(strings = {"test", "http_gw_01"})
	void processMOMessageWhenGatewayIsNullOrDisabledThenReturnDefaultTemplate(String testSystemId) {
		Map<String, Object> bodyRequest = this.generateMoMessage(true);
		bodyRequest.put("system_id",testSystemId);

		if ("http_gw_01".equals(testSystemId)) {
			gatewayConcurrentHashMap.get(testSystemId).setEnabled(0);
		}

		assertThrows(SmsProcessingException.class, () -> messageEventService.processRequest(bodyRequest, "/message", MediaType.APPLICATION_JSON_VALUE));
	}

	@ParameterizedTest
	@DisplayName("Process MO message when interpreter not found then message is received")
	@ValueSource(strings = {"message", "response"})
	void processMOMessageWhenInterpreterNotFoundThenReturnDefaultTemplate(String testType) {
		Gateway gateway = gatewayConcurrentHashMap.get(systemId);
		List<PayloadMapper> interpreter = gateway.getInterpreter();
		PayloadMapper payloadMapper;
		if ("message".equals(testType)) {
			payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), MESSAGE_EVENT_TYPE, INPUT_DIRECTION);
		} else {
			payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), RESPONSE_MESSAGE_EVENT_TYPE, OUTPUT_DIRECTION);
		}

		interpreter.remove(payloadMapper);
		this.gatewayConcurrentHashMap.get(systemId).setInterpreter(interpreter);
		processMessageService = new ProcessMessageService(appProperties, gatewayConcurrentHashMap, interpreterByPathRequestConcurrentHashMap, kafkaTemplate, proxyResponseHandler);

		Map<String, Object> request = this.generateMoMessage(true);
		assertThrows(NoInterpreterFoundException.class, () -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		if ("response".equals(testType)) {
			verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC), anyString());
		}
	}

	@ParameterizedTest
	@SuppressWarnings("unchecked")
	@DisplayName("Process DLR with default and custom template then successfully")
	@ValueSource(strings = {"1719421854353-11028072268459", "12345"})
	void processDlrWithDefaultAndCustomTemplateThenDoSuccessfully(String messageId) {

		if ("1719421854353-11028072268459".equals(messageId)) {
			when(gatewayHttpConnection.getGateway()).thenReturn(getGatewayWithInterpreter());
		}

		MessageEvent eventResult = MessageEvent.builder()
				.messageId(messageId)
				.systemId(systemId)
				.destNetworkId(1)
				.build();
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId)).thenReturn(eventResult.toString());

		Map<String, Object> response = messageEventService.processRequest(this.generateDlrRequest(messageId), "/message", MediaType.APPLICATION_JSON_VALUE);
		assertNotNull(response);
		Map<String, String> body = (Map<String, String>) response.get("body");
		assertTrue(body.containsKey("message_id") && messageId.equals(body.get("message_id")));
		verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
	}

	@ParameterizedTest
	@DisplayName("Process DLR with default template when gateway is null and interpreter response drl not found and message id not found")
	@ValueSource(strings = {"1719421854353-11028072260000", "1719421854353-11028072261111", "1719421854353-11028072268459" })
	void processDlrWithDefaultTemplateAndGatewayIsNullThenResponseWithADefaultTemplateResponse(String messageId) {

		Gateway gateway = this.gatewayConcurrentHashMap.get(systemId);
		if ("1719421854353-11028072268459".equals(messageId)) {
			this.httpConnectionManagerList.clear();
		}

		if ("1719421854353-11028072260000".equals(messageId)) {
			List<PayloadMapper> interpreter = gateway.getInterpreter();
			PayloadMapper payloadMapper = InterpreterUtils.findInterpreterByEventType(gateway.getInterpreter(), RESPONSE_DLR_EVENT_TYPE, OUTPUT_DIRECTION);
			interpreter.remove(payloadMapper);
			this.gatewayConcurrentHashMap.get(systemId).setInterpreter(interpreter);
			gateway.setInterpreter(interpreter);
			when(gatewayHttpConnection.getGateway()).thenReturn(gateway);
			this.httpConnectionManagerList.put("1", gatewayHttpConnection);
		}

		MessageEvent eventResult = MessageEvent.builder()
				.messageId(messageId)
				.systemId(systemId)
				.destNetworkId(1)
				.build();
		String redisEvent = eventResult.toString();
		if ("1719421854353-11028072261111".equals(messageId)) {
			redisEvent = null;
		}
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId)).thenReturn(redisEvent);

		processMessageDlrService = new ProcessMessageDlrService(interpreterByPathRequestConcurrentHashMap, gatewayConcurrentHashMap, httpConnectionManagerList, scyllaManager, kafkaTemplate, proxyResponseHandler, appProperties);
		processMessageService = new ProcessMessageService(appProperties, gatewayConcurrentHashMap, interpreterByPathRequestConcurrentHashMap, kafkaTemplate, proxyResponseHandler);

		Map<String, Object> request = this.generateDlrRequest(messageId);
		if ("1719421854353-11028072260000".equals(messageId)) {
			assertThrows(NoInterpreterFoundException.class, () -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		} else {
			assertThrows(SmsProcessingException.class, () -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		}

		if ("1719421854353-11028072260000".equals(messageId)) {
			// no interpreter response found
			verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
		} else {
			// gateway was not found | Message id was not found
			verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
		}
	}

	@Test
	@DisplayName("Process dlr with a custom template when gateways is null then dlr is processed but the response has a default template")
	void processMessageDlrWithCustomTemplateWhenGatewaysIsNullThenResponseWithADefaultTemplateResponse() {
		gatewayConcurrentHashMap.clear();
		String messageId= "12345";
		MessageEvent eventResult = MessageEvent.builder()
				.messageId(messageId)
				.systemId(systemId)
				.destNetworkId(1)
				.build();
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId)).thenReturn(eventResult.toString());

		processMessageDlrService = new ProcessMessageDlrService(interpreterByPathRequestConcurrentHashMap, gatewayConcurrentHashMap, httpConnectionManagerList, scyllaManager, kafkaTemplate, proxyResponseHandler, appProperties);
		Map<String, Object> request = this.generateDlrRequest(messageId);
		assertThrows(SmsProcessingException.class, () -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		verify(kafkaTemplate, never()).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
	}

	@Test
	@DisplayName("Process DLR in proxy mode holds the response until the deliver_sm confirmation arrives")
	void processDlrInProxyModeWaitsForDeliverSmConfirmation() {
		String messageId = "1719421854353-11028072268459";
		String submitSmServerId = "1719421854353-99990000000001";
		when(gatewayHttpConnection.getGateway()).thenReturn(getGatewayWithInterpreter());
		when(appProperties.getProxyModeResponseTimeout()).thenReturn(2000L);
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId))
				.thenReturn(proxySubmitResult(messageId, submitSmServerId));

		// Mimic the smpp-server publishing the confirmation after the ESME acks the deliver_sm.
		Thread.startVirtualThread(() -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			proxyResponseHandler.completeFuture(submitSmServerId,
					new UtilsRecords.HttpProxyResponse(submitSmServerId, false, 0, "", null));
		});

		Map<String, Object> response = messageEventService.processRequest(
				this.generateDlrRequest(messageId), "/message", MediaType.APPLICATION_JSON_VALUE);

		assertNotNull(response);
		verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), contains("\"use_proxy\":true"));
	}

	@Test
	@DisplayName("Process DLR in proxy mode fails when no deliver_sm confirmation arrives before the timeout")
	void processDlrInProxyModeTimesOutWithoutConfirmation() {
		String messageId = "1719421854353-11028072268459";
		when(gatewayHttpConnection.getGateway()).thenReturn(getGatewayWithInterpreter());
		when(appProperties.getProxyModeResponseTimeout()).thenReturn(50L);
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId))
				.thenReturn(proxySubmitResult(messageId, "1719421854353-99990000000002"));

		Map<String, Object> request = this.generateDlrRequest(messageId);
		assertThrows(SmsDeliveryException.class,
				() -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
		verify(kafkaTemplate).send(eq(KafkaTopicsConstants.PRE_DELIVER_TOPIC), anyString());
	}

	@Test
	@DisplayName("Process DLR in proxy mode fails when the deliver_sm confirmation reports an error")
	void processDlrInProxyModeFailsOnErrorConfirmation() {
		String messageId = "1719421854353-11028072268459";
		String submitSmServerId = "1719421854353-99990000000003";
		when(gatewayHttpConnection.getGateway()).thenReturn(getGatewayWithInterpreter());
		when(appProperties.getProxyModeResponseTimeout()).thenReturn(2000L);
		when(scyllaManager.selectFromTable(ScyllaTablesConstants.HTTP_SUBMIT_SM_RESULT_TABLE, messageId))
				.thenReturn(proxySubmitResult(messageId, submitSmServerId));

		Thread.startVirtualThread(() -> {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			proxyResponseHandler.completeFuture(submitSmServerId,
					new UtilsRecords.HttpProxyResponse(submitSmServerId, true, 8, "deliver_sm failed", null));
		});

		Map<String, Object> request = this.generateDlrRequest(messageId);
		assertThrows(SmsDeliveryException.class,
				() -> messageEventService.processRequest(request, "/message", MediaType.APPLICATION_JSON_VALUE));
	}

	private String proxySubmitResult(String carrierMessageId, String submitSmServerId) {
		return new UtilsRecords.SubmitSmResponseEvent(
				carrierMessageId,
				"1719421854353-1",
				systemId,
				carrierMessageId,
				submitSmServerId,
				"SMPP",
				1,
				"SP",
				"",
				null,
				null,
				submitSmServerId,
				1,
				false,
				Map.of(com.client.http.utils.Constants.PROXY_MODE_CUSTOM_PARAM, true),
				false,
				"",
				"",
				"HIGH"
		).toString();
	}

    private Map<String, Object>  generateDlrRequest(String messageId) {
        Map<String, Object> dlrRequest = new HashMap<>();

		if ("12345".equals(messageId)) {
			dlrRequest.put("custom_message_id", messageId);
		} else {
			dlrRequest.put("message_id", messageId);
		}

        dlrRequest.put("source_addr_ton", 1);
        dlrRequest.put("source_addr", "50510201020");
        dlrRequest.put("destination_addr_ton", 1);
        dlrRequest.put("destination_addr", "50582368999");
        dlrRequest.put("status", "UNDELIV");
        dlrRequest.put("error_code", "500");


        return dlrRequest;
    }

	private  Map<String, Object> generateMoMessage(boolean includeSystemId) {
		Map<String, Object> messages = new HashMap<>();
		if (includeSystemId) {
			messages.put("system_id", systemId);
		} else {
			messages.put("custom_system_id", systemId);
		}

		messages.put("source_addr_ton", 1);
		messages.put("source_addr", "50510201020");
		messages.put("destination_addr_ton", 1);
		messages.put("destination_addr", "50582368999");
		messages.put("short_message", "This a test message");
		messages.put("data_coding", 8);
		messages.put("esm_class", "0");

		return messages;
	}

	private InterpreterByPathRequest createInterpreterForMOMessage() {
        PayloadMapper payloadMapper = new PayloadMapper();
        payloadMapper.setPath("/message");
        payloadMapper.setBodyType("JSON");
        payloadMapper.setEventType("message");
        payloadMapper.setDirection("input");
        payloadMapper.setDefaultTemplate(true);
        payloadMapper.setTemplate(this.getMessageTemplate());
        InterpreterByPathRequest interpreterByPathRequest = new InterpreterByPathRequest();
        interpreterByPathRequest.setSystemId(systemId);
        interpreterByPathRequest.setPayloadMappers(payloadMapper);

		return interpreterByPathRequest;
    }

	private InterpreterByPathRequest createInterpreterForDLRMessage() {
		PayloadMapper payloadMapper = new PayloadMapper();
		payloadMapper.setPath("/message");
		payloadMapper.setBodyType("JSON");
		payloadMapper.setEventType("dlr");
		payloadMapper.setDirection("input");
		payloadMapper.setDefaultTemplate(true);
		payloadMapper.setTemplate(this.getDlrTemplate());
		InterpreterByPathRequest interpreterByPathRequest = new InterpreterByPathRequest();
		interpreterByPathRequest.setSystemId(systemId);
		interpreterByPathRequest.setPayloadMappers(payloadMapper);

		return interpreterByPathRequest;
	}

    private Gateway getGatewayWithInterpreter() {
        return Gateway.builder()
                .networkId(1)
                .name("HTTP-Operator")
                .systemId(systemId)
                .sessionsNumber(10)
                .tps(10)
                .successSession(0)
                .status("STARTED")
                .enabled(1)
                .enquireLinkPeriod(30000)
                .enquireLinkTimeout(0)
                .requestDLR(1)
                .protocol("HTTP")
				.interpreter(this.createInterpreterToGateway())
                .build();
    }

    private String getMessageTemplate() {
        return """
				{
						"system_id": "{{systemId:STRING}}",
						"source_addr_ton": "{{sourceAddrTon:INT}}",
						"source_addr": "{{sourceAddr:STRING}}",
						"dest_addr_ton": "{{destAddrTon:INT}}",
						"destination_addr": "{{destinationAddr:STRING}}",
						"esm_class": "{{esmClass:INT}}",
						"data_coding": "{{dataCoding:INT}}",
						"short_message": "{{shortMessage:STRING}}",
						"custom_system_id": "{{systemId:STRING}}"
				}
				""";
    }

	private String getDlrTemplate() {
		return """
				{
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
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_message_id": "{{messageId:STRING}}"
				}
				""";
	}

	private List<PayloadMapper> createInterpreterToGateway() {
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
				  "system_id": "{{systemId:STRING}}",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr:STRING}}",
				  "esm_class": "{{esmClass:INT}}",
				  "validity_period": "{{validityPeriod:LONG}}",
				  "registered_delivery": "{{registeredDelivery:INT}}",
				  "data_coding": "{{dataCoding:INT}}",
				  "sm_default_msg_id": "{{smDefaultMsgId:INT}}",
				  "short_message": "{{shortMessage:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_parameters": "{{customParams:MAP}}"
				},
			
				"message|output": {
				  "message_id": "{{messageId:STRING}}",
				  "source_addr_ton": "{{sourceAddrTon:INT}}",
				  "source_addr_npi": "{{sourceAddrNpi:INT}}",
				  "source_addr": "{{sourceAddr:STRING}}",
				  "dest_addr_ton": "{{destAddrTon:INT}}",
				  "dest_addr_npi": "{{destAddrNpi:INT}}",
				  "destination_addr": "{{destinationAddr:STRING}}",
				  "registered_delivery": "{{registeredDelivery:INT}}",
				  "data_coding": "{{dataCoding:INT}}",
				  "short_message": "{{shortMessage:STRING}}",
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_parameters": "{{customParams:MAP}}"
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
				  "segment_sequence": "{{segmentSequence:INT}}"
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
				  "optional_parameters": "{{optionalParameters:LIST}}",
				  "custom_message_id": "{{messageId:STRING}}"
				},
			
				"response_message|input": {
				  "message_id": "{{messageId:STRING}}"
				},
			
				"response_message|output": {
				  "system_id": "{{systemId:STRING}}",
				  "message_id": "{{messageId:STRING}}",
				  "message": "{{shortMessage:STRING}}"
				},
			
				"response_dlr|output": {
				  "system_id": "{{systemId:STRING}}",
				  "message_id": "{{messageId:STRING}}",
				  "message": "{{shortMessage:STRING}}"
				}
			}
			""";

		List<PayloadMapper> payloadMappers = new ArrayList<>();
		JsonNode templates = Converter.stringToObject(interpreterDefaultTemplate, JsonNode.class);
		JsonNode defaultPath = Converter.stringToObject(interpreterDefaultPath, JsonNode.class);

		for (String key : interpreterKeyTemplate) {
			PayloadMapper payloadMapper = new PayloadMapper();

			String[] parts = key.split("\\|");
			String eventType = parts[0];
			String direction = parts[1];

			if (defaultPath.has(key)) {
				payloadMapper.setPath(defaultPath.get(key).asText());
			}
			payloadMapper.setEventType(eventType);
			payloadMapper.setDirection(direction);
			payloadMapper.setBodyType("JSON");
			payloadMapper.setUseProxy(false);
			payloadMapper.setTemplate(templates.get(key).toString());
			payloadMapper.setDefaultTemplate(true);

			payloadMappers.add(payloadMapper);
		}

		return payloadMappers;
	}
}