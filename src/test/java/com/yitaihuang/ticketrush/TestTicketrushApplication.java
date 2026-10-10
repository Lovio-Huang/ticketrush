package com.yitaihuang.ticketrush;

import org.springframework.boot.SpringApplication;

public class TestTicketrushApplication {

	public static void main(String[] args) {
		SpringApplication.from(TicketrushApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
