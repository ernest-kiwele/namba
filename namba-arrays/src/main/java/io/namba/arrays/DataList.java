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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 */
public class DataList<T> implements Iterable<T>, NambaList {

	protected final List<T> value;
	protected final DataType dataType;
	protected final Index index;

	protected DataList(DataType dataType, List<T> is) {
		this.value = Collections.unmodifiableList(is);
		this.dataType = dataType;
		this.index = null;
	}

	private DataList(DataType dataType, T[] is) {
		this(dataType, Arrays.asList(is));
	}

	protected DataList(DataType dataType, List<T> is, Index index) {
		this.value = Collections.unmodifiableList(is);
		this.dataType = dataType;
		this.index = index;
	}

	@Override
	public final int size() {
		return this.value.size();
	}

	@Override
	public final DataType dataType() {
		return this.dataType;
	}

	@Override
	public Index index() {
		return this.index;
	}

	public static <T> DataList<T> of(DataType dataType, T... values) {
		return new DataList<>(dataType, values);
	}

	public DataList<T> getAt(int[] is) {
		List<T> l = new ArrayList<>();
		for (int i : is) {
			l.add(value.get(i));
		}
		return new DataList<>(this.dataType, l);
	}

	public T getAt(int is) {
		return this.value.get(is);
	}

	public DataList<T> getAt(IntRange range) {
		return new DataList<>(this.dataType, range.stream().mapToObj(this.value::get).collect(Collectors.toList()));
	}

	public DataList<T> getAt(List<Integer> range) {
		return new DataList<>(this.dataType, range.stream().map(this.value::get).collect(Collectors.toList()));
	}

	// operations
	public Stream<T> stream() {
		return values().stream();
	}

	public Collection<T> values() {
		return Collections.unmodifiableList(this.value);
	}

	public Iterator<T> iterator() {
		return values().iterator();
	}

	// Wrapper
	public DataList<T> zip(DataList<T> other, BinaryOperator<T> op) {
		if (this.size() != other.size()) {
			throw new IllegalStateException("Arrays are of different sizes");
		}

		List<T> list = new ArrayList<>();
		Iterator<T> it1 = this.iterator(), it2 = other.iterator();
		while (it1.hasNext()) {
			list.add(op.apply(it1.next(), it2.next()));
		}

		return new DataList<>(this.dataType, list);
	}

	public static <T> DataList<T> zip(DataList<T> a, DataList<T> b, BinaryOperator<T> op) {
		return a.zip(b, op);
	}

	public <U> DataList<U> map(Function<T, U> op) {
		return new DataList<>(this.dataType,
				this.stream().map(v -> v == null ? null : op.apply(v)).collect(Collectors.toList()));
	}

	public <U> DataList<U> apply(Function<T, U> op) {
		return this.map(op);
	}

	// tests
	public IntList test(Predicate<T> p) {
		return IntList.of(this.value.stream().mapToInt(v -> p.test(v) ? 1 : 0).toArray());
	}

	public int count(Predicate<T> p) {
		return this.value.stream().mapToInt(v -> p.test(v) ? 1 : 0).sum();
	}

	public boolean all(Predicate<T> p) {
		return this.value.stream().allMatch(p);
	}

	public boolean any(Predicate<T> p) {
		return this.value.stream().anyMatch(p);
	}

	public T item(int loc) {
		return this.getAt(loc);
	}

	public List<T> toList() {
		return this.value;
	}

	@Override
	public NambaList repeat(int n) {
		return new DataList<T>(this.dataType, IntStream.range(0, n).mapToObj(i -> this.value.stream())
				.flatMap(Function.identity()).collect(Collectors.toList()));
	}

	@Override
	public StringList string() {
		return StringList
				.of(this.value.stream().map(v -> v == null ? null : v.toString()).collect(Collectors.toList()));
	}

	@Override
	public String toString() {
		return this.value.stream().toString();
	}
}