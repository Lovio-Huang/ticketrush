package com.yitaihuang.ticketrush.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Renders every error as an RFC 9457 problem detail. Validation and malformed
 * request errors are handled by the {@link ResponseEntityExceptionHandler} base.
 * Nothing from an exception's message reaches the client: messages of unexpected
 * and infrastructure errors can carry SQL, addresses or data, so they are logged.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

	private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

	@ExceptionHandler(ApiException.class)
	ResponseEntity<ProblemDetail> handleApiException(ApiException ex) {
		return problem(ex.getStatus(), ex.getCode(), ex.getMessage());
	}

	/** Two requests raced on the same order, e.g. "pay" while the expiry job ran. */
	@ExceptionHandler(ObjectOptimisticLockingFailureException.class)
	ResponseEntity<ProblemDetail> handleOptimisticLock(ObjectOptimisticLockingFailureException ex) {
		return problem(HttpStatus.CONFLICT, "CONCURRENT_MODIFICATION",
				"The order was modified by another request, fetch it and try again");
	}

	/**
	 * Redis or MySQL unreachable, or a command that timed out. A timeout doesn't
	 * mean nothing happened (a reservation script may have run after all), so the
	 * client is told to retry with the same Idempotency-Key, which returns the
	 * original order if there is one.
	 */
	@ExceptionHandler({ TransientDataAccessException.class, DataAccessResourceFailureException.class,
			RecoverableDataAccessException.class })
	ResponseEntity<ProblemDetail> handleUnavailable(DataAccessException ex) {
		log.warn("Temporarily unavailable: {}", ex.toString());
		ResponseEntity<ProblemDetail> response = problem(HttpStatus.SERVICE_UNAVAILABLE, "TEMPORARILY_UNAVAILABLE",
				"A backing service is temporarily unavailable. The request may or may not have been applied: "
						+ "retry it with the same Idempotency-Key.");
		return ResponseEntity.status(response.getStatusCode()).header("Retry-After", "1").body(response.getBody());
	}

	/** Anything else is a bug: logged in full, answered without details. */
	@ExceptionHandler(Exception.class)
	ResponseEntity<ProblemDetail> handleUnexpected(Exception ex) throws Exception {
		if (ex instanceof AccessDeniedException || ex instanceof AuthenticationException) {
			throw ex; // Spring Security's filters answer these with 401/403
		}
		log.error("Unhandled exception", ex);
		return problem(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Something went wrong on our side");
	}

	/** Validation and framework errors get a {@code code} too, so clients can always switch on it. */
	@Override
	protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
			HttpStatusCode statusCode, WebRequest request) {
		ResponseEntity<Object> response = super.handleExceptionInternal(ex, body, headers, statusCode, request);
		if (response != null && response.getBody() instanceof ProblemDetail problem
				&& (problem.getProperties() == null || !problem.getProperties().containsKey("code"))) {
			HttpStatus status = HttpStatus.resolve(statusCode.value());
			String code = statusCode.value() == 400 ? "INVALID_REQUEST" : status != null ? status.name() : "ERROR";
			problem.setProperty("code", code);
		}
		return response;
	}

	private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String code, String detail) {
		ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
		body.setTitle(code);
		body.setProperty("code", code);
		return ResponseEntity.status(status).body(body);
	}

}
