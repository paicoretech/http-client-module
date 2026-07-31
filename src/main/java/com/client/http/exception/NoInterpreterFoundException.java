package com.client.http.exception;

import com.client.http.utils.Utils;
import lombok.Getter;
import org.springframework.http.MediaType;

@Getter
public class NoInterpreterFoundException extends RuntimeException {
  private final MediaType mediaType;
  private final transient Object response;

  public NoInterpreterFoundException(String message, MediaType mediaType) {
    super(message);
    this.mediaType = mediaType;
    this.response = Utils.prepareResponse(message, mediaType);
  }
}
