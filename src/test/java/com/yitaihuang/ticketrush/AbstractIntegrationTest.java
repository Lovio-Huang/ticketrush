package com.yitaihuang.ticketrush;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.yitaihuang.ticketrush.event.CreateEventRequest;
import com.yitaihuang.ticketrush.event.EventService;

/**
 * Boots the whole application against real infrastructure. All subclasses share
 * one Spring context (and one set of containers) thanks to context caching.
 * Skipped automatically on machines without Docker; CI always has it.
 */
@SpringBootTest(properties = "ticketrush.orders.expiry-scan-interval=PT1H")
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractIntegrationTest {

	@Autowired
	protected EventService eventService;

	@Autowired
	protected StringRedisTemplate redis;

	@BeforeEach
	void resetRateLimits() {
		var keys = redis.keys("ticketrush:ratelimit:*");
		if (!keys.isEmpty()) {
			redis.delete(keys);
		}
	}

	protected long createEvent(int tickets, int perUserLimit) {
		Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
		return eventService.create(new CreateEventRequest("Integration test", "Test Arena",
				now.plus(30, ChronoUnit.DAYS), now.minus(1, ChronoUnit.MINUTES), tickets, 2500, perUserLimit))
			.id();
	}

}
