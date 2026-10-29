package com.yitaihuang.ticketrush.common;

import java.util.List;
import java.util.function.Function;

import org.springframework.data.domain.Page;

/** A stable JSON shape for paged results, independent of Spring Data's Page serialization. */
public record PageResponse<T>(List<T> items, int page, int size, long totalItems, int totalPages) {

	public static <S, T> PageResponse<T> of(Page<S> page, Function<S, T> mapper) {
		return new PageResponse<>(page.map(mapper).getContent(), page.getNumber(), page.getSize(),
				page.getTotalElements(), page.getTotalPages());
	}

}
