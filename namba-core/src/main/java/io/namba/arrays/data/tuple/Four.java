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

/**
 * 
 * @author Ernest Kiwele
 */
public class Four<A, B, C, D> implements Tuple {

	private final A a;
	private final B b;
	private final C c;
	private final D d;

	private Four(A a, B b, C c, D d) {
		this.a = a;
		this.b = b;
		this.c = c;
		this.d = d;
	}

	public static <A, B, C, D> Four<A, B, C, D> of(A a, B b, C c, D d) {
		return new Four<>(a, b, c, d);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getAt(int level) {
		switch (level) {
			case 0:
				return (T) this.a;
			case 1:
				return (T) this.b;
			case 2:
				return (T) this.c;
			case 3:
				return (T) this.d;
			default:
				throw new IllegalArgumentException("Level " + level + " is invalid");
		}
	}

	public A a() {
		return a;
	}

	public B b() {
		return b;
	}

	public C c() {
		return c;
	}

	public D d() {
		return d;
	}

	public A getA() {
		return a();
	}

	public B getB() {
		return b();
	}

	public C getC() {
		return c();
	}

	public D getD() {
		return d();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((a == null) ? 0 : a.hashCode());
		result = prime * result + ((b == null) ? 0 : b.hashCode());
		result = prime * result + ((c == null) ? 0 : c.hashCode());
		result = prime * result + ((d == null) ? 0 : d.hashCode());
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
		Four other = (Four) obj;
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
		if (c == null) {
			if (other.c != null)
				return false;
		} else if (!c.equals(other.c))
			return false;
		if (d == null) {
			if (other.d != null)
				return false;
		} else if (!d.equals(other.d))
			return false;
		return true;
	}
}
