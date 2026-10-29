package com.yitaihuang.ticketrush.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.UUID;

import javax.crypto.spec.SecretKeySpec;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.yitaihuang.ticketrush.AbstractIntegrationTest;

class AuthIntegrationTest extends AbstractIntegrationTest {

	@Autowired
	MockMvcTester mvc;

	@Autowired
	JwtEncoder encoder;

	/** Usernames are unique across the whole test run (one shared database). */
	private static String unique(String prefix) {
		return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
	}

	private MvcTestResult post(String path, String username, String password) {
		return mvc.post()
			.uri(path)
			.contentType(MediaType.APPLICATION_JSON)
			.content("{\"username\":\"%s\",\"password\":\"%s\"}".formatted(username, password))
			.exchange();
	}

	@Test
	void registerLogInAndAskWhoIAm() throws Exception {
		String name = unique("alice");

		assertThat(post("/api/auth/register", name, "correct horse")).hasStatus(HttpStatus.CREATED)
			.bodyJson()
			.extractingPath("$.scopes")
			.asArray()
			.containsExactly("user");
		MvcTestResult login = post("/api/auth/login", name, "correct horse");
		assertThat(login).hasStatusOk().bodyJson().extractingPath("$.tokenType").isEqualTo("Bearer");
		String token = login.getResponse().getContentAsString().replaceAll(".*\"accessToken\":\"([^\"]+)\".*", "$1");

		assertThat(mvc.get().uri("/api/auth/me").header("Authorization", "Bearer " + token)).hasStatusOk()
			.bodyJson()
			.extractingPath("$.username")
			.isEqualTo(name);
	}

	@Test
	void usernamesAreCaseInsensitive() {
		String name = unique("Mixed.Case");

		assertThat(post("/api/auth/register", name, "a long password")).hasStatus(HttpStatus.CREATED);
		assertThat(post("/api/auth/register", name.toLowerCase(), "another password")).hasStatus(HttpStatus.CONFLICT)
			.bodyJson()
			.extractingPath("$.code")
			.isEqualTo("USERNAME_TAKEN");
	}

	@Test
	void aWrongPasswordAndAnUnknownUserGetTheSameAnswer() throws Exception {
		String name = unique("bob");
		post("/api/auth/register", name, "the real one");

		MvcTestResult wrongPassword = post("/api/auth/login", name, "not the one");
		MvcTestResult noSuchUser = post("/api/auth/login", unique("nobody"), "not the one");

		assertThat(wrongPassword).hasStatus(HttpStatus.UNAUTHORIZED).bodyJson().extractingPath("$.code")
			.isEqualTo("INVALID_CREDENTIALS");
		assertThat(noSuchUser.getResponse().getContentAsString())
			.isEqualTo(wrongPassword.getResponse().getContentAsString());
	}

	@Test
	void passwordGuessingIsThrottled() {
		String name = unique("carol");
		post("/api/auth/register", name, "the real one");

		int throttled = 0;
		for (int i = 0; i < 20; i++) {
			if (post("/api/auth/login", name, "guess " + i).getResponse().getStatus() == 429) {
				throttled++;
			}
		}
		assertThat(throttled).isPositive();
	}

	@Test
	void aPasswordLongerThanBcryptReadsIsRefused() {
		// 30 characters, 90 bytes in UTF-8: BCrypt only reads the first 72 bytes.
		assertThat(post("/api/auth/register", unique("dave"), "密".repeat(30))).hasStatus(HttpStatus.BAD_REQUEST);
	}

	@Test
	void withoutATokenTheApiAnswers401WithAProblem() {
		MvcTestResult result = mvc.get().uri("/api/orders").exchange();

		assertThat(result).hasStatus(HttpStatus.UNAUTHORIZED).hasHeader("WWW-Authenticate", "Bearer");
		assertThat(result).bodyJson().extractingPath("$.code").isEqualTo("UNAUTHORIZED");
	}

	@Test
	void forgedExpiredAndForeignTokensAreRejected() {
		byte[] otherKey = Base64.getDecoder().decode("c29tZW9uZSBlbHNlJ3Mga2V5LCAzMiBieXRlcyBsb25nIQ==");
		JwtEncoder forger = new NimbusJwtEncoder(new ImmutableSecret<>(new SecretKeySpec(otherKey, "HmacSHA256")));
		Instant now = Instant.now();

		String forged = sign(forger, claims("ticketrush", now.plus(1, ChronoUnit.HOURS)));
		String expired = sign(encoder, claims("ticketrush", now.minus(1, ChronoUnit.HOURS)));
		String foreign = sign(encoder, claims("someone-else", now.plus(1, ChronoUnit.HOURS)));

		for (String token : new String[] { forged, expired, foreign }) {
			assertThat(mvc.get().uri("/api/orders").header("Authorization", "Bearer " + token))
				.hasStatus(HttpStatus.UNAUTHORIZED);
		}
	}

	@Test
	void onlyAdminsCreateEventsAndReadSalesStats() {
		long eventId = createEvent(10, 2);
		String event = """
				{"name":"Gig","venue":"Hall","startsAt":"%s","saleStartsAt":"%s",
				 "totalTickets":10,"priceCents":100,"perUserLimit":2}
				""".formatted(Instant.now().plus(30, ChronoUnit.DAYS), Instant.now());

		assertThat(mvc.post().uri("/api/events").header("Authorization", bearer("eve"))
			.contentType(MediaType.APPLICATION_JSON).content(event)).hasStatus(HttpStatus.FORBIDDEN)
			.bodyJson().extractingPath("$.code").isEqualTo("FORBIDDEN");
		assertThat(mvc.get().uri("/api/events/{id}/stats", eventId).header("Authorization", bearer("eve")))
			.hasStatus(HttpStatus.FORBIDDEN);
		assertThat(mvc.get().uri("/api/events/{id}/stats", eventId).header("Authorization", adminBearer()))
			.hasStatusOk();
	}

	private static JwtClaimsSet claims(String issuer, Instant expiresAt) {
		return JwtClaimsSet.builder()
			.issuer(issuer)
			.subject("42")
			.issuedAt(expiresAt.minus(2, ChronoUnit.HOURS))
			.expiresAt(expiresAt)
			.claim("scope", "user")
			.build();
	}

	private static String sign(JwtEncoder encoder, JwtClaimsSet claims) {
		return encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims))
			.getTokenValue();
	}

}
