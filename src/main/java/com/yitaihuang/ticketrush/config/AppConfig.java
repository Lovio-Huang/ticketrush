package com.yitaihuang.ticketrush.config;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AppConfig {

	/** Injected everywhere "now" matters so tests can control time. */
	@Bean
	Clock clock() {
		return Clock.systemUTC();
	}

}
