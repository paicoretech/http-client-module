package com.client.http.service;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageEventService {
	private final ProcessMessageDlrService processMessageDlrService;
	private final ProcessMessageService processMessageService;

	public Map<String, Object> processRequest(Map<String, Object> bodyRequest, String pathRequest, String contentTypeHeader) {
		String baseContentType = Arrays.stream(contentTypeHeader.split(";")).findFirst().orElse("");
		Map<String, Object> processRequest = processMessageDlrService.processDlr(bodyRequest, pathRequest, MediaType.valueOf(baseContentType));
		if (Objects.isNull(processRequest) || processRequest.isEmpty()) {
			processRequest = processMessageService.processMessage(bodyRequest, pathRequest, MediaType.valueOf(baseContentType));
		}

		return processRequest;
	}
}
