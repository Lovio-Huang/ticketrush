package com.yitaihuang.ticketrush.auth;

public enum Role {

	USER("user"),

	/** Can create events and see sales stats, on top of everything a user can do. */
	ADMIN("user admin");

	private final String scopes;

	Role(String scopes) {
		this.scopes = scopes;
	}

	/** Space-separated OAuth2 scopes; Spring turns each into a {@code SCOPE_x} authority. */
	public String scopes() {
		return scopes;
	}

}
