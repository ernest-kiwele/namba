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

package io.namba.arrays.data.tuple;

import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import lombok.Data;

/**
 * 
 * @author Ernest Kiwele
 *
 */
@Data
public class TwoInts {

	private final int a;
	private final int b;

	private TwoInts(int a, int b) {
		this.a = a;
		this.b = b;
	}

	public static TwoInts of(int a, int b) {
		return new TwoInts(a, b);
	}

	public int getAt(int level) {
		switch (level) {
			case 0:
				return this.a;
			case 1:
				return this.b;
			default:
				throw new IllegalArgumentException("Level " + level + " is invalid");
		}
	}

	public int a() {
		return this.a;
	}

	public int b() {
		return this.b;
	}

	public int getA() {
		return a();
	}

	public int getB() {
		return b();
	}

	public static <A, B> Collector<Two<A, B>, ?, Map<A, B>> mapCollector() {
		return Collectors.toMap(Two<A, B>::a, Two<A, B>::b);
	}

	@Override
	public String toString() {
		return "TwoInts[a=" + a + ", b=" + b + "]";
	}
}
