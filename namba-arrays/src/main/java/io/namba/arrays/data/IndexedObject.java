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
public class IndexedObject<T extends Comparable<T>> implements Comparable<IndexedObject<T>> {
	private final int index;
	private final T value;

	private IndexedObject(int index, T value) {
		this.index = index;
		this.value = value;
	}

	public static <T extends Comparable<T>> IndexedObject<T> of(int i, T value) {
		return new IndexedObject<>(i, value);
	}

	public int index() {
		return this.index;
	}

	public T value() {
		return this.value;
	}

	public int getIndex() {
		return index;
	}

	public T getValue() {
		return value;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean equals(Object obj) {
		if (!(obj instanceof IndexedObject))
			return false;

		IndexedObject<T> o = (IndexedObject) obj;
		return this.value != null && o.value != null && this.value.equals(o.value);
	}

	@Override
	public int hashCode() {
		return this.value == null ? 0 : this.value.hashCode();
	}

	@Override
	public int compareTo(IndexedObject<T> o) {
		return this.value.compareTo(o.value);
	}
}
