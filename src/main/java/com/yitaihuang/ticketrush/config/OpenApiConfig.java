package com.yitaihuang.ticketrush.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;

/** Interactive API docs at /swagger-ui.html, with an "Authorize" button for the JWT. */
@Configuration(proxyBeanMethods = false)
public class OpenApiConfig {

	@Bean
	OpenAPI ticketrushOpenApi() {
		return new OpenAPI()
			.info(new Info().title("TicketRush API")
				.version("1.0")
				.description("Flash-sale ticketing: log in via /api/auth/login, then click Authorize and paste "
						+ "the accessToken.")
				.license(new License().name("MIT")))
			.components(new Components().addSecuritySchemes("bearer",
					new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("bearer").bearerFormat("JWT")))
			.addSecurityItem(new SecurityRequirement().addList("bearer"));
	}

}
