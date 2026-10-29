package com.yitaihuang.ticketrush.auth;

import java.time.Clock;
import java.util.Locale;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.yitaihuang.ticketrush.common.ApiException;
import com.yitaihuang.ticketrush.config.TicketrushProperties;
import com.yitaihuang.ticketrush.reservation.RateLimiter;

@Service
public class AuthService {

	private final UserRepository users;

	private final PasswordEncoder passwords;

	private final TokenService tokens;

	private final RateLimiter rateLimiter;

	private final TicketrushProperties.Security security;

	private final Clock clock;

	/**
	 * Hash checked when the username doesn't exist, so "no such user" takes as
	 * long as "wrong password" and response times don't reveal which accounts exist.
	 */
	private final String dummyHash;

	public AuthService(UserRepository users, PasswordEncoder passwords, TokenService tokens, RateLimiter rateLimiter,
			TicketrushProperties properties, Clock clock) {
		this.users = users;
		this.passwords = passwords;
		this.tokens = tokens;
		this.rateLimiter = rateLimiter;
		this.security = properties.security();
		this.clock = clock;
		this.dummyHash = passwords.encode("timing-equalizer");
	}

	@Transactional
	public User register(String username, String password, Role role) {
		String normalized = normalize(username);
		if (users.existsByUsername(normalized)) {
			throw ApiException.conflict("USERNAME_TAKEN", "That username is already taken");
		}
		try {
			return users.saveAndFlush(new User(normalized, passwords.encode(password), role, clock.instant()));
		}
		catch (DataIntegrityViolationException raceWithAnotherSignup) {
			throw ApiException.conflict("USERNAME_TAKEN", "That username is already taken");
		}
	}

	@Transactional(readOnly = true)
	public TokenService.AccessToken login(String username, String password) {
		String normalized = normalize(username);
		// Per account, much stricter than the reservation limit: a few guesses a minute, not a few a second.
		if (!rateLimiter.tryAcquirePerMinute("login:" + normalized, security.loginAttemptsPerMinute())) {
			throw ApiException.tooManyRequests("RATE_LIMITED", "Too many login attempts, try again shortly");
		}
		User user = users.findByUsername(normalized).orElse(null);
		boolean valid = passwords.matches(password, user != null ? user.getPasswordHash() : dummyHash);
		if (user == null || !valid) {
			throw new ApiException(HttpStatus.UNAUTHORIZED, "INVALID_CREDENTIALS", "Wrong username or password");
		}
		return tokens.issue(user.getId(), user.getUsername(), user.getRole());
	}

	/**
	 * Caps logins and sign-ups per client address: one address trying many accounts, or
	 * creating accounts to burn CPU on BCrypt. Behind a reverse proxy, enable
	 * {@code server.forward-headers-strategy} so the address is the client's, not the proxy's.
	 */
	public void checkAddress(String address) {
		if (!rateLimiter.tryAcquirePerMinute("auth-address:" + address, security.authRequestsPerMinutePerAddress())) {
			throw ApiException.tooManyRequests("RATE_LIMITED", "Too many attempts from this address, try again shortly");
		}
	}

	static String normalize(String username) {
		return username.trim().toLowerCase(Locale.ROOT);
	}

}
