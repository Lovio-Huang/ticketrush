package com.yitaihuang.ticketrush.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;

import org.junit.jupiter.api.Test;

/** What the app does with each kind of JWT_SECRET it can be started with. */
class SecurityConfigTest {

	@Test
	void anUnsetOrEmptySecretFallsBackToTheDevelopmentKey() {
		byte[] dev = Base64.getDecoder().decode(SecurityConfig.DEV_SECRET);

		assertThat(SecurityConfig.signingKey(null).getEncoded()).isEqualTo(dev);
		assertThat(SecurityConfig.signingKey("").getEncoded()).isEqualTo(dev); // compose passes an unset variable as ""
	}

	@Test
	void aCustomSecretIsUsedAsIs() {
		byte[] custom = new byte[48];
		custom[0] = 7;

		assertThat(SecurityConfig.signingKey(Base64.getEncoder().encodeToString(custom)).getEncoded()).isEqualTo(custom);
	}

	@Test
	void aMalformedOrShortSecretStopsTheAppWithoutEchoingIt() {
		String notBase64 = "not-base64-but-long-enough-to-look-like-a-key!!";
		assertThatThrownBy(() -> SecurityConfig.signingKey(notBase64)).isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("JWT_SECRET")
			.hasMessageNotContaining(notBase64)
			.hasNoCause();
		String tooShort = Base64.getEncoder().encodeToString(new byte[16]);
		assertThatThrownBy(() -> SecurityConfig.signingKey(tooShort)).isInstanceOf(IllegalStateException.class)
			.hasMessageContaining("32 bytes");
	}

}
