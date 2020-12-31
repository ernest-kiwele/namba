/**
 * Copyright 2018 eussence.com and contributors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.namba.arrays;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

/**
 * 
 * @author Ernest Kiwele
 */
public enum DataType {

	INT("int", "Int", "32-bit Integer", int.class),

	LONG("long", "Long", "64-bit Integer", long.class),

	DOUBLE("double", "Double", "64-bit Double", double.class),

	BIGINT("bigint", "BigInt", "Big Integer", BigInteger.class),

	BIGDECIMAL("bigdecimal", "BigDecimal", "Big Decimal", BigDecimal.class),

	STRING("string", "String", "String", String.class),

	CATEGORY("category", "Category", "Enumeration-like", String.class),

	DATE("date", "Date", "System-local date", LocalDate.class),

	TIME("time", "Time", "System-local time", Time.class),

	DATETIME("datetime", "Datetime", "System-local date time", LocalDateTime.class),

	INSTANT("instant", "Instant", "UTC instant", java.time.Instant.class),

	BOOLEAN("boolean", "Boolean", "Boolean", boolean.class),

	OBJECT("object", "Object", "Arbitrary user-defined element type", Object.class);

	private static final Map<String, DataType> mapping = new ConcurrentHashMap<>();

	private final String key;
	private final String displayName;
	private final String description;
	private final Class<?> javaType;

	private DataType(String key, String displayName, String description, Class<?> javaType) {
		this.key = key;
		this.displayName = displayName;
		this.description = description;
		this.javaType = javaType;
	}

	public String getKey() {
		return key;
	}

	public String getDisplayName() {
		return displayName;
	}

	public String getDescription() {
		return description;
	}

	public Class<?> getJavaType() {
		return javaType;
	}

	public static DataType of(String name) {
		return mapping.computeIfAbsent(name, n -> Arrays.stream(values())
				.filter(v -> StringUtils.equalsIgnoreCase(v.key, n)).findAny().orElse(null));
	}

	@Override
	public String toString() {
		return this.key;
	}
}
