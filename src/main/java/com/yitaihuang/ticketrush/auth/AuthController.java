package com.yitaihuang.ticketrush.auth;

import java.nio.charset.StandardCharsets;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

	private final AuthService auth;

	public AuthController(AuthService auth) {
		this.auth = auth;
	}

	public record Credentials(
			@NotBlank @Size(min = 3, max = 32) @Pattern(regexp = "[A-Za-z0-9_.-]+",
					message = "may only contain letters, digits, '.', '_' and '-'") String username,
			@NotBlank @Size(min = 8, max = 72) String password) {

		/**
		 * BCrypt reads only the first 72 bytes, and Spring's encoder throws (a 500) rather than
		 * cut silently. 72 characters isn't the same thing: 30 Chinese characters are 90 bytes.
		 */
		@AssertTrue(message = "password must be at most 72 bytes in UTF-8")
		public boolean isPasswordWithinBcryptLimit() {
			return password == null || password.getBytes(StandardCharsets.UTF_8).length <= 72;
		}
	}

	public record UserResponse(long id, String username, List<String> scopes) {
	}

	@PostMapping("/register")
	@ResponseStatus(HttpStatus.CREATED)
	public UserResponse register(@Valid @RequestBody Credentials credentials, HttpServletRequest request) {
		auth.checkAddress(request.getRemoteAddr());
		User user = auth.register(credentials.username(), credentials.password(), Role.USER);
		return new UserResponse(user.getId(), user.getUsername(), List.of(user.getRole().scopes().split(" ")));
	}

	@PostMapping("/login")
	public TokenService.AccessToken login(@Valid @RequestBody Credentials credentials, HttpServletRequest request) {
		auth.checkAddress(request.getRemoteAddr());
		return auth.login(credentials.username(), credentials.password());
	}

	@GetMapping("/me")
	public UserResponse me(@AuthenticationPrincipal Jwt jwt) {
		return new UserResponse(Long.parseLong(jwt.getSubject()), jwt.getClaimAsString("name"),
				List.of(jwt.getClaimAsString("scope").split(" ")));
	}

}
