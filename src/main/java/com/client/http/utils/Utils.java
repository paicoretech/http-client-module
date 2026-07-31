package com.client.http.utils;

import com.paicbd.smsc.utils.Generated;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;

import java.util.Map;

@Slf4j
@Generated
public class Utils {
    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    public static Object prepareResponse(String message, MediaType mediaType) {
        if (MediaType.APPLICATION_JSON.equals(mediaType)) {
            return Map.of(
                    "status", "error",
                    "message", message
            );
        } else {
            return """
                    <response>
                        <status>error</status>
                        <message>%s</message>
                    </response>
                    """.formatted(message);
        }
    }
}
