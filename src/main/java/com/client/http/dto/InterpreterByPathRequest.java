package com.client.http.dto;

import com.paicbd.smsc.interpreter.PayloadMapper;
import com.paicbd.smsc.utils.Generated;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter @Setter
@RequiredArgsConstructor
@AllArgsConstructor
@Generated
public class InterpreterByPathRequest {
    private String systemId;
    private PayloadMapper payloadMappers;
}
