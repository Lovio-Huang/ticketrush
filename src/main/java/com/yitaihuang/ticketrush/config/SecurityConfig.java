package com.yitaihuang.ticketrush.config;

import java.io.IOException;
import java.util.Base64;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import jakarta.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.header.writers.DelegatingRequestMatcherHeaderWriter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.security.web.servlet.util.matcher.PathPatternRequestMatcher;
import org.springframework.security.web.util.matcher.OrRequestMatcher;

import com.nimbusds.jose.jwk.source.ImmutableSecret;
import com.yitaihuang.ticketrush.auth.TokenService;

import tools.jackson.databind.json.JsonMapper;

/**
 * Stateless JWT security. The app is its own token issuer (HS256 with a shared
 * key); swapping in an external identity provider would only mean replacing
 * the decoder with one that reads the provider's JWKS endpoint.
 */
@Configuration(proxyBeanMethods = false)
public class SecurityConfig {

	private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

	/** Committed only so the project runs out of the box. Override with JWT_SECRET anywhere real. */
	static final String DEV_SECRET = "46nTHWwbEfvhyRNt4EVzb0fs8bOHvuq7D2Wao7h9qn0=";

	/**
	 * Scripts only from this origin, so injected markup can't run code and read the token.
	 * Inline styles stay allowed: the console sets bar widths with style attributes, and CSS
	 * can't read storage.
	 */
	static final String CONSOLE_CSP = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
			+ "img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; "
			+ "frame-ancestors 'none'";

	@Bean
	SecurityFilterChain securityFilterChain(HttpSecurity http, JsonMapper json) throws Exception {
		AuthenticationEntryPoint unauthorized = (request, response, ex) -> {
			response.setHeader("WWW-Authenticate", "Bearer");
			writeProblem(response, json, HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", "Missing or invalid access token");
		};
		AccessDeniedHandler forbidden = (request, response, ex) -> writeProblem(response, json, HttpStatus.FORBIDDEN,
				"FORBIDDEN", "You don't have permission to do that");

		http.csrf(AbstractHttpConfigurer::disable)
			.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
			.authorizeHttpRequests(auth -> auth
				.requestMatchers("/api/auth/register", "/api/auth/login").permitAll()
				.requestMatchers("/api/events/*/stats").hasAuthority("SCOPE_admin")
				.requestMatchers(HttpMethod.POST, "/api/events").hasAuthority("SCOPE_admin")
				.requestMatchers(HttpMethod.GET, "/api/events", "/api/events/*").permitAll()
				.requestMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus").permitAll()
				.requestMatchers("/", "/index.html", "/assets/**", "/favicon.ico", "/error").permitAll()
				.requestMatchers("/swagger-ui.html", "/swagger-ui/**", "/v3/api-docs/**").permitAll()
				.anyRequest().authenticated())
			.oauth2ResourceServer(oauth -> oauth.jwt(Customizer.withDefaults())
				.authenticationEntryPoint(unauthorized)
				.accessDeniedHandler(forbidden))
			.exceptionHandling(ex -> ex.authenticationEntryPoint(unauthorized).accessDeniedHandler(forbidden))
			// The console keeps its token in localStorage, where any injected script could read it.
			// Only the console's own pages get the policy: Swagger UI needs inline scripts.
			.headers(headers -> headers.addHeaderWriter(new DelegatingRequestMatcherHeaderWriter(
					new OrRequestMatcher(PathPatternRequestMatcher.pathPattern("/"),
							PathPatternRequestMatcher.pathPattern("/index.html"),
							PathPatternRequestMatcher.pathPattern("/assets/**")),
					new StaticHeadersWriter("Content-Security-Policy", CONSOLE_CSP))));
		return http.build();
	}

	@Bean
	SecretKey jwtSigningKey(TicketrushProperties properties) {
		String secret = properties.security().jwtSecret();
		if (DEV_SECRET.equals(secret)) {
			log.warn("Using the built-in development JWT secret. Set JWT_SECRET before deploying anywhere real.");
		}
		byte[] bytes = Base64.getDecoder().decode(secret);
		if (bytes.length < 32) {
			throw new IllegalStateException("JWT secret must be at least 256 bits (32 bytes, base64-encoded)");
		}
		return new SecretKeySpec(bytes, "HmacSHA256");
	}

	@Bean
	JwtEncoder jwtEncoder(SecretKey key) {
		return new NimbusJwtEncoder(new ImmutableSecret<>(key));
	}

	@Bean
	JwtDecoder jwtDecoder(SecretKey key) {
		NimbusJwtDecoder decoder = NimbusJwtDecoder.withSecretKey(key).macAlgorithm(MacAlgorithm.HS256).build();
		// Checks expiry, not-before and that we issued the token ourselves.
		decoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(TokenService.ISSUER));
		return decoder;
	}

	@Bean
	PasswordEncoder passwordEncoder() {
		return new BCryptPasswordEncoder();
	}

	private static void writeProblem(HttpServletResponse response, JsonMapper json, HttpStatus status, String code,
			String detail) throws IOException {
		ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
		problem.setTitle(code);
		problem.setProperty("code", code);
		response.setStatus(status.value());
		response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
		json.writeValue(response.getOutputStream(), problem);
	}

}
