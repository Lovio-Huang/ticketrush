package com.yitaihuang.ticketrush.auth;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.stereotype.Service;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/**
 * Issues short-lived HS256 access tokens. Stateless: any instance can verify a
 * token with the shared key, so there is no session store to scale.
 */
@Service
public class TokenService {

	public static final String ISSUER = "ticketrush";

	private final JwtEncoder encoder;

	private final Duration ttl;

	private final Clock clock;

	public TokenService(JwtEncoder encoder, TicketrushProperties properties, Clock clock) {
		this.encoder = encoder;
		this.ttl = properties.security().tokenTtl();
		this.clock = clock;
	}

	public record AccessToken(String accessToken, String tokenType, long expiresIn) {
	}

	public AccessToken issue(long userId, String username, Role role) {
		Instant now = clock.instant();
		JwtClaimsSet claims = JwtClaimsSet.builder()
			.issuer(ISSUER)
			.subject(String.valueOf(userId))
			.issuedAt(now)
			.expiresAt(now.plus(ttl))
			.claim("name", username)
			.claim("scope", role.scopes())
			.build();
		JwsHeader header = JwsHeader.with(MacAlgorithm.HS256).build();
		String token = encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
		return new AccessToken(token, "Bearer", ttl.toSeconds());
	}

}
