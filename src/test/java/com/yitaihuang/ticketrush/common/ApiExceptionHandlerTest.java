package com.yitaihuang.ticketrush.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import org.junit.jupiter.api.Test;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Every error a client sees is a problem detail with a stable code, and nothing internal. */
class ApiExceptionHandlerTest {

	@RestController
	static class Failing {

		@GetMapping("/bug")
		void bug() {
			throw new IllegalStateException("select * from users where password = 'hunter2' at jdbc:mysql://10.0.0.7");
		}

		@GetMapping("/redis-down")
		void redisDown() {
			throw new RedisConnectionFailureException("Unable to connect to 10.0.0.5:6379");
		}

		@GetMapping("/redis-timeout")
		void redisTimeout() {
			throw new QueryTimeoutException("Redis command timed out after 1 second(s)");
		}

		@GetMapping("/mysql-down")
		void mysqlDown() {
			throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection to jdbc:mysql://10.0.0.7");
		}

		@GetMapping("/missing")
		void missing() {
			throw ApiException.notFound("ORDER_NOT_FOUND", "Order x does not exist");
		}

		@GetMapping("/denied")
		void denied() {
			throw new AccessDeniedException("method security says no");
		}

	}

	private final MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new Failing())
		.setControllerAdvice(new ApiExceptionHandler())
		.build();

	private final MockMvcTester mvc = MockMvcTester.create(mockMvc);

	@Test
	void anUnexpectedErrorIsAGeneric500WithoutItsMessage() throws Exception {
		var response = mvc.get().uri("/bug").exchange();

		assertThat(response).hasStatus(HttpStatus.INTERNAL_SERVER_ERROR)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("INTERNAL_ERROR");
		assertThat(response.getResponse().getContentAsString()).doesNotContain("hunter2", "jdbc", "10.0.0.7",
				"IllegalState", "select");
	}

	@Test
	void anUnreachableOrSlowRedisOrMysqlIsARetryable503() throws Exception {
		for (String path : new String[] { "/redis-down", "/redis-timeout", "/mysql-down" }) {
			var response = mvc.get().uri(path).exchange();

			assertThat(response).as(path).hasStatus(HttpStatus.SERVICE_UNAVAILABLE);
			assertThat(response).bodyJson().extractingPath("$.code").isEqualTo("TEMPORARILY_UNAVAILABLE");
			assertThat(response.getResponse().getContentAsString()).as(path)
				.contains("Idempotency-Key")
				.doesNotContain("10.0.0", "jdbc", "6379", "timed out after");
		}
	}

	@Test
	void businessErrorsKeepTheirStatusAndCode() {
		assertThat(mvc.get().uri("/missing")).hasStatus(HttpStatus.NOT_FOUND)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("ORDER_NOT_FOUND");
	}

	@Test
	void anAccessDeniedIsLeftToSpringSecurityNotTurnedInto500() {
		// In the app the security filter chain turns it into the 403 problem detail.
		assertThatThrownBy(() -> mockMvc.perform(get("/denied"))).hasRootCauseInstanceOf(AccessDeniedException.class);
	}

}
