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

package io.namba.arrays.data;

import java.util.Comparator;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class IntPair {

	private final int a;
	private final int b;

	private IntPair(int a, int b) {
		this.a = a;
		this.b = b;
	}

	public static IntPair of(int a, int b) {
		return new IntPair(a, b);
	}

	public int a() {
		return this.a;
	}

	public int b() {
		return this.b;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(this.a * this.b);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IntPair))
			return false;
		IntPair p = (IntPair) obj;

		return this.a == p.a && this.b == p.b;
	}

	public static Comparator<IntPair> comparingByA() {
		return Comparator.comparingInt(IntPair::a);
	}

	public static Comparator<IntPair> comparingByB() {
		return Comparator.comparingInt(IntPair::b);
	}
}
