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
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.namba.arrays.data.tuple.Two;

/**
 * 
 * @author Ernest Kiwele
 */
public class Mask implements NambaList {

	private final boolean[] value;
	private final Index index;

	private Mask(boolean[] array, Index index) {
		this.value = array;
		this.index = index;
	}

	private Mask(boolean[] array) {
		this(array, null);
	}

	public static Mask of(boolean[] array) {
		return new Mask(array, null);
	}

	public static Mask trues(int size) {
		boolean[] b = new boolean[size];
		Arrays.fill(b, true);
		return new Mask(b);
	}

	public static Mask falses(int size) {
		boolean[] b = new boolean[size];
		Arrays.fill(b, false);
		return new Mask(b);
	}

	public static Mask of(Boolean[] array) {
		boolean[] b = new boolean[array.length];
		for (int i = 0; i < array.length; i++)
			b[i] = array[i];

		return new Mask(b);
	}

	public static Mask of(int size, IntPredicate predicate) {
		boolean[] b = new boolean[size];

		for (int i = 0; i < size; i++) {
			b[i] = predicate.test(i);
		}

		return of(b);
	}

	@Override
	public DataType dataType() {
		return DataType.BOOLEAN;
	}

	@Override
	public Mask getAt(int[] loc) {
		boolean[] r = new boolean[loc.length];
		for (int i = 0; i < r.length; i++) {
			r[i] = this.value[loc[i]];
		}
		return Mask.of(r);
	}

	public boolean getAt(int loc) {
		return this.value[loc];
	}

	@Override
	public Index index() {
		return this.index;
	}

	@Override
	public Mask repeat(int n) {
		boolean[] v = new boolean[n * this.value.length];

		for (int i = 0; i < n; i++) {
			System.arraycopy(this.value, 0, v, i * this.value.length, this.value.length);
		}

		return Mask.of(v);
	}

	@Override
	public int size() {
		return this.value.length;
	}

	@Override
	public StringList string() {
		return StringList.of(IntStream.range(0, this.value.length).mapToObj(i -> this.value[i] ? "true" : "false")
				.collect(Collectors.toList()));
	}

	@Override
	public String toString() {
		return Arrays.toString(this.value);
	}

	/**
	 * Count true values
	 * 
	 * @return
	 */
	public int count() {
		return (int) IntStream.range(0, this.value.length).filter(i -> this.value[i]).count();
	}

	public int trueCount() {
		return this.count();
	}

	/**
	 * Count false values
	 * 
	 * @return
	 */
	public int falseCount() {
		return this.value.length - this.count();
	}

	public IntList asInt() {
		return IntList.of(IntStream.range(0, this.value.length).map(i -> this.value[i] ? 1 : 0).toArray());
	}

	public IntList truthy() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> this.value[i]).toArray());
	}

	public IntList falsy() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> !this.value[i]).toArray());
	}

	public boolean all() {
		return IntStream.range(0, this.value.length).allMatch(i -> this.value[i]);
	}

	public boolean any() {
		return IntStream.range(0, this.value.length).anyMatch(i -> this.value[i]);
	}

	public boolean none() {
		return IntStream.range(0, this.value.length).noneMatch(i -> this.value[i]);
	}

	public boolean anyFalse() {
		return IntStream.range(0, this.value.length).anyMatch(i -> !this.value[i]);
	}

	public int sum() {
		return this.asInt().sum().orElse(0);
	}

	public double mean() {
		if (0 == this.size()) {
			return 0.0;
		}
		return ((double) this.sum()) / this.size();
	}

	public double meanPercent() {
		return ((double) this.sum()) / this.size() * 100;
	}

	public Mask and(Mask other) {
		boolean[] r = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++)
			r[i] = this.value[i] && other.value[i];

		return Mask.of(r);
	}

	public Mask or(Mask other) {
		boolean[] r = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++)
			r[i] = this.value[i] || other.value[i];

		return Mask.of(r);
	}

	public Mask xor(Mask other) {
		boolean[] r = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++)
			r[i] = this.value[i] ^ other.value[i];

		return Mask.of(r);
	}

	public Mask negate() {
		boolean[] r = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++)
			r[i] = !this.value[i];

		return Mask.of(r);
	}

	public Mask negative() {
		return this.negate();
	}

	public Mask bitwiseNegate() {
		return this.negate();
	}

	public <E, C extends NambaList> C apply(IntFunction<E> trueFunction, IntFunction<E> falseFunction,
			Function<List<E>, C> listGenerator) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			values.add(this.value[i] ? trueFunction.apply(i) : falseFunction.apply(i));
		}

		return listGenerator.apply(values);
	}

	public <E, C extends NambaList> C apply(IntFunction<E> trueFunction, Function<List<E>, C> listGenerator) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			values.add(this.value[i] ? trueFunction.apply(i) : null);
		}

		return listGenerator.apply(values);
	}

	public <E> List<E> apply(IntFunction<E> trueFunction, IntFunction<E> falseFunction) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			values.add(this.value[i] ? trueFunction.apply(i) : falseFunction.apply(i));
		}

		return values;
	}

	public <E> List<E> apply(IntFunction<E> trueFunction) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			values.add(this.value[i] ? trueFunction.apply(i) : null);
		}

		return values;
	}

	public <E> List<E> applyWhereTrue(IntFunction<E> function) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			if (this.value[i])
				values.add(function.apply(i));
		}

		return values;
	}

	public <E, C extends NambaList> C applyWhereTrue(IntFunction<E> f, Function<List<E>, C> listGenerator) {
		List<E> values = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			if (this.value[i])
				values.add(f.apply(i));
		}

		return listGenerator.apply(values);
	}

	public <E, C extends NambaList> Two<C, C> partition(DataList<E> data, Function<List<E>, C> listGenerator) {
		List<E> trues = new ArrayList<>();
		List<E> falses = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			if (this.value[i])
				trues.add(data.getAt(i));
			else
				falses.add(data.getAt(i));
		}

		return Two.of(listGenerator.apply(trues), listGenerator.apply(falses));
	}

	@Override
	public LongList asLong() {
		long[] data = new long[this.size()];
		for (int i = 0; i < this.size(); i++) {
			data[i] = this.value[i] ? 1 : 0;
		}
		return LongList.of(data);
	}

	@Override
	public DoubleList asDouble() {
		return this.asLong().asDouble();
	}

	@Override
	public Mask asMask() {
		return this;
	}
}
