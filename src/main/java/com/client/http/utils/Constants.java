package com.client.http.utils;

import com.paicbd.smsc.utils.Generated;

@Generated
public class Constants {
    private Constants() {
        throw new IllegalStateException("Utility class");
    }

    public static final String STOPPED = "STOPPED";
    public static final String UPDATE_GATEWAY_ENDPOINT = "/app/http/updateGateway";
    public static final String CONNECT_GATEWAY_ENDPOINT = "/app/http/connectGateway";
    public static final String RESPONSE_SMPP_CLIENT_ENDPOINT = "/app/response-smpp-client";
    public static final String STOP_GATEWAY_ENDPOINT = "/app/http/stopGateway";
    public static final String DELETE_GATEWAY_ENDPOINT = "/app/http/deleteGateway";
    public static final String UPDATE_ERROR_CODE_MAPPING_ENDPOINT = "/app/updateErrorCodeMapping"; // Receive mno_id as String
    public static final String PARAM_UPDATE_STATUS = "status";
    public static final int IS_STARTED = 1;

    // interpreter
    public static final String MESSAGE_EVENT_TYPE = "message";
    public static final String DLR_EVENT_TYPE = "dlr";
    public static final String RESPONSE_MESSAGE_EVENT_TYPE = "response_message";
    public static final String RESPONSE_DLR_EVENT_TYPE = "response_dlr";
    public static final String INPUT_DIRECTION = "input";
    public static final String OUTPUT_DIRECTION = "output";
    public static final String ORIGIN_GATEWAY_TYPE = "GW";

    // Custom param persisted with the submit result when the origin requested proxy mode,
    // so the DLR callback flow holds its HTTP response until the deliver_sm is confirmed.
    public static final String PROXY_MODE_CUSTOM_PARAM = "use_proxy";
}
