package com.yitaihuang.ticketrush;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

/** What a browser or API client sees around the business endpoints: console, docs, error shape. */
class ApiSurfaceIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	MockMvcTester mvc;

	@Test
	void theConsoleIsServedWithAPolicyThatBlocksInjectedScripts() {
		var page = mvc.get().uri("/").exchange();

		assertThat(page).hasStatusOk();
		assertThat(page.getResponse().getHeader("Content-Security-Policy")).contains("script-src 'self'")
			.contains("frame-ancestors 'none'");
	}

	@Test
	void apiDocsArePublicAndDescribeTheBearerToken() {
		assertThat(mvc.get().uri("/v3/api-docs")).hasStatusOk()
			.bodyJson()
			.extractingPath("$.components.securitySchemes.bearer.scheme")
			.isEqualTo("bearer");
		assertThat(mvc.get().uri("/swagger-ui.html").exchange().getResponse().getStatus()).isIn(200, 302);
	}

	@Test
	void validationErrorsCarryACodeLikeEveryOtherError() {
		long eventId = createEvent(10, 4);

		assertThat(mvc.post()
			.uri("/api/events/{id}/reservations", eventId)
			.header("Authorization", bearer("lena"))
			.header("Idempotency-Key", "zero")
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"quantity\":0}")).hasStatus(HttpStatus.BAD_REQUEST)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("INVALID_REQUEST");
	}

	@Test
	void signUpsAreThrottledPerClientAddress() {
		int throttled = 0;
		for (int i = 0; i < 40; i++) {
			int status = mvc.post()
				.uri("/api/auth/register")
				.contentType(MediaType.APPLICATION_JSON)
				.content("{\"username\":\"bulk-%d-%d\",\"password\":\"long enough\"}".formatted(i, System.nanoTime()))
				.exchange()
				.getResponse()
				.getStatus();
			throttled += status == 429 ? 1 : 0;
		}
		assertThat(throttled).isPositive();

		// Another address is unaffected.
		assertThat(mvc.post()
			.uri("/api/auth/register")
			.with(request -> {
				request.setRemoteAddr("203.0.113.7");
				return request;
			})
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"username\":\"elsewhere-%d\",\"password\":\"long enough\"}".formatted(System.nanoTime())))
			.hasStatus(HttpStatus.CREATED);
	}

}
