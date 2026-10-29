package com.yitaihuang.ticketrush.auth;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.yitaihuang.ticketrush.config.TicketrushProperties;

/** Creates the first admin account from configuration, so a fresh deployment can create events. */
@Component
public class AdminBootstrap implements ApplicationRunner {

	private static final Logger log = LoggerFactory.getLogger(AdminBootstrap.class);

	private final AuthService auth;

	private final UserRepository users;

	private final TicketrushProperties.Security security;

	public AdminBootstrap(AuthService auth, UserRepository users, TicketrushProperties properties) {
		this.auth = auth;
		this.users = users;
		this.security = properties.security();
	}

	@Override
	public void run(ApplicationArguments args) {
		if (!StringUtils.hasText(security.adminUsername()) || !StringUtils.hasText(security.adminPassword())) {
			return;
		}
		if (users.existsByUsername(AuthService.normalize(security.adminUsername()))) {
			return;
		}
		// Configuration skips the API's validation, so check what sign-up would have.
		int bytes = security.adminPassword().getBytes(StandardCharsets.UTF_8).length;
		if (security.adminPassword().length() < 8 || bytes > 72) {
			throw new IllegalStateException("ADMIN_PASSWORD must be at least 8 characters and at most 72 bytes");
		}
		auth.register(security.adminUsername(), security.adminPassword(), Role.ADMIN);
		log.info("Created admin account '{}'", security.adminUsername());
	}

}
