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

/**
 * 
 * @author Ernest Kiwele
 */
public class Two<A, B> implements Tuple {

	private final A a;
	private final B b;

	private Two(A a, B b) {
		this.a = a;
		this.b = b;
	}

	public static <A, B> Two<A, B> of(A a, B b) {
		return new Two<>(a, b);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getAt(int level) {
		switch (level) {
			case 0:
				return (T) this.a;
			case 1:
				return (T) this.b;
			default:
				throw new IllegalArgumentException("Level " + level + " is invalid");
		}
	}

	public A a() {
		return this.a;
	}

	public B b() {
		return this.b;
	}

	public A getA() {
		return a();
	}

	public B getB() {
		return b();
	}

	public static <A, B> Collector<Two<A, B>, ?, Map<A, B>> mapCollector() {
		return Collectors.toMap(Two<A, B>::a, Two<A, B>::b);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((a == null) ? 0 : a.hashCode());
		result = prime * result + ((b == null) ? 0 : b.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Two other = (Two) obj;
		if (a == null) {
			if (other.a != null)
				return false;
		} else if (!a.equals(other.a))
			return false;
		if (b == null) {
			if (other.b != null)
				return false;
		} else if (!b.equals(other.b))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "T[a=" + a + ", b=" + b + "]";
	}
}
