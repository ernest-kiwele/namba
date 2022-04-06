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

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class IndexedLong implements Comparable<IndexedLong> {

	private final int index;
	private final long value;

	private IndexedLong(int index, long value) {
		this.index = index;
		this.value = value;
	}

	public static IndexedLong of(int i, long value) {
		return new IndexedLong(i, value);
	}

	public int index() {
		return this.index;
	}

	public long value() {
		return this.value;
	}

	public int getIndex() {
		return index;
	}

	public long getValue() {
		return value;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof IndexedLong))
			return false;

		IndexedLong o = (IndexedLong) obj;
		return this.value == o.value;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(value);
	}

	@Override
	public int compareTo(IndexedLong o) {
		return Long.compare(this.value, o.value);
	}
}
