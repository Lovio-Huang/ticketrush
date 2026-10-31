package com.yitaihuang.ticketrush.common;

import org.springframework.http.HttpStatus;

/**
 * A business error that maps directly to an HTTP response. {@code code} is a
 * stable, machine-readable identifier clients can switch on.
 */
public class ApiException extends RuntimeException {

	private final HttpStatus status;

	private final String code;

	public ApiException(HttpStatus status, String code, String message) {
		super(message);
		this.status = status;
		this.code = code;
	}

	public static ApiException badRequest(String code, String message) {
		return new ApiException(HttpStatus.BAD_REQUEST, code, message);
	}

	public static ApiException unprocessable(String code, String message) {
		return new ApiException(HttpStatus.UNPROCESSABLE_ENTITY, code, message);
	}

	public static ApiException notFound(String code, String message) {
		return new ApiException(HttpStatus.NOT_FOUND, code, message);
	}

	public static ApiException conflict(String code, String message) {
		return new ApiException(HttpStatus.CONFLICT, code, message);
	}

	public static ApiException tooManyRequests(String code, String message) {
		return new ApiException(HttpStatus.TOO_MANY_REQUESTS, code, message);
	}

	public static ApiException serviceUnavailable(String code, String message) {
		return new ApiException(HttpStatus.SERVICE_UNAVAILABLE, code, message);
	}

	public HttpStatus getStatus() {
		return status;
	}

	public String getCode() {
		return code;
	}

}
