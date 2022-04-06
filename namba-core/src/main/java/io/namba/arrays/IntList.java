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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.namba.arrays.agg.IntGrouping;
import io.namba.arrays.data.IndexedObject;
import io.namba.arrays.data.IntData;
import io.namba.arrays.data.IntPair;
import io.namba.arrays.data.tuple.Two;
import io.namba.arrays.data.tuple.TwoInts;
import io.namba.arrays.range.IntRange;
import io.namba.functions.IntRef;
import io.namba.functions.IntRef.IntListPredicate;

/**
 * 
 * @author Ernest Kiwele
 */
public class IntList implements NambaList {

	protected final int[] value;
	protected final Index index;

	public final IntList.IndexAccessor idx = new IndexAccessor();
	public final IntList.RadixAccessor radix = new RadixAccessor();

	private IntList(int size) {
		this.value = new int[size];
		this.index = null;
	}

	protected IntList(int[] val) {
		this.value = Objects.requireNonNull(val);
		this.index = null;
	}

	private IntList(int[] val, IntFunction<Object> indexer) {
		this.value = Objects.requireNonNull(val);
		this.index = Index.intIndex(this.value, indexer);
	}

	@Override
	public int size() {
		return this.value.length;
	}

	@Override
	public DataType dataType() {
		return DataType.INT;
	}

	@Override
	public Index index() {
		return this.index;
	}

	public static IntList of(int size, int value) {
		IntList array = new IntList(size);
		Arrays.fill(array.value, value);
		return array;
	}

	public static IntList of(int[] value) {
		return new IntList(value);
	}

	// indexing
	public IntList indexBy(IntFunction<Object> indexer) {
		return new IntList(this.value, Objects.requireNonNull(indexer, "indexer is null"));
	}

	public IntList getByIndex(Object key) {
		if (null == this.index) {
			throw new IllegalStateException("array is not indexed");
		}

		return this.getAt(this.index.getByKey(key));
	}

	@Override
	public Index getIndex() {
		return index;
	}

	// utilities
	public IntMatrix toMatrix(int width) {
		return new IntMatrix(this.value, width);
	}

	@Override
	public String toString() {
		return Arrays.toString(this.value);
	}

	// accessors
	private IntList getAt(IntStream stream) {
		return IntList.of(Objects.requireNonNull(stream).filter(i -> i >= 0 && i < this.value.length)
				.map(i -> this.value[i]).toArray());
	}

	public IntList getAt(Mask mask) {
		return this.getAt(mask.truthy().value);
	}

	@Override
	public IntList getAt(int[] is) {
		return getAt(Arrays.stream(is));
	}

	public IntRef where(IntListPredicate p) {
		return IntRef.where(p, this);
	}

	public IntList take(int size) {
		return getAt(IntRange.of(size));
	}

	@SuppressWarnings("unchecked")
	public <T> T getAt(int loc) {
		return (T) Integer.valueOf(this.value[loc]);
	}

	public IntList getAt(IntRange range) {
		return getAt(range.stream());
	}

	public IntList getAt(List<Integer> range) {
		return getAt(range.stream().mapToInt(Integer::intValue));
	}

	public IntStream stream() {
		return Arrays.stream(value);
	}

	public Stream<Two<Integer, Integer>> indexedStream() {
		return IntStream.range(0, size()).mapToObj(i -> Two.of(i, this.getAt(i)));
	}

	// operations
	public IntList zip(IntList other, IntBinaryOperator op) {
		if (this.value.length != other.value.length) {
			throw new IllegalArgumentException("arrays are not of the same length");
		}

		int[] res = new int[this.value.length];
		for (int i = 0; i < res.length; i++) {
			res[i] = op.applyAsInt(this.value[i], other.value[i]);
		}

		return of(res);
	}

	public IntList map(IntUnaryOperator op) {
		IntUnaryOperator operation = Objects.requireNonNull(op);
		int[] n = new int[this.value.length];
		for (int i = 0; i < this.value.length; i++) {
			n[i] = operation.applyAsInt(this.value[i]);
		}
		return IntList.of(n);
	}

	public IntList apply(IntUnaryOperator op) {
		return this.map(op);
	}

	public IntList multiply(int n) {
		return this.map(i -> i * n);
	}

	public IntList multiply(IntList n) {
		return this.zip(n, (a, b) -> a * b);
	}

	public IntList minus(int n) {
		return this.map(i -> i - n);
	}

	public IntList minus(IntList n) {
		return this.zip(n, (a, b) -> a - b);
	}

	public IntList plus(int n) {
		return this.map(i -> i + n);
	}

	public IntList plus(IntList n) {
		return this.zip(n, (a, b) -> a + b);
	}

	public IntList divide(int n) {
		return this.map(i -> i / n);
	}

	public IntList divide(IntList n) {
		return this.zip(n, (a, b) -> a / b);
	}

	public IntList power(int n) {
		return this.map(i -> (int) Math.pow(i, n));
	}

	public IntList power(IntList n) {
		return this.zip(n, (a, b) -> (int) Math.pow(a, b));
	}

	public IntList abs() {
		int[] r = new int[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = Math.abs(this.value[i]);

		return of(r);
	}

	public IntList absolute() {
		return this.abs();
	}

	public IntList negative() {
		int[] r = new int[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = -this.value[i];

		return of(r);
	}

	public IntList positive() {
		return this;
	}

	public IntList mod(int other) {
		int[] r = new int[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = this.value[i] % other;

		return of(r);
	}

	public IntList mod(IntList n) {
		return this.zip(n, (a, b) -> a % b);
	}

	public IntList signum() {
		int[] r = new int[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = Integer.signum(this.value[i]);

		return of(r);
	}

	public IntList sign() {
		return this.signum();
	}

	public int mode() {
		return IntStream.range(0, this.value.length).mapToObj(i -> this.value[i])
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().stream()
				.max(Map.Entry.comparingByValue()).orElseThrow().getKey();
	}

	public DoubleList sin() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::sin).toArray());
	}

	public DoubleList cos() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::cos).toArray());
	}

	public DoubleList tan() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::tan).toArray());
	}

	public DoubleList arcsin() {
		return this.asin();
	}

	public DoubleList asin() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::asin).toArray());
	}

	public DoubleList arccos() {
		return this.acos();
	}

	public DoubleList acos() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::acos).toArray());
	}

	public DoubleList arctan() {
		return this.acos();
	}

	public DoubleList atan() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::atan).toArray());
	}

	public DoubleList hsin() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::sinh).toArray());
	}

	public DoubleList sinh() {
		return this.hsin();
	}

	public DoubleList hcos() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::cosh).toArray());
	}

	public DoubleList cosh() {
		return this.hcos();
	}

	public DoubleList htan() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::tanh).toArray());
	}

	public DoubleList tanh() {
		return this.htan();
	}

	public DoubleList rad() {
		return DoubleList.of(IntStream.of(this.value).mapToDouble(Math::toRadians).toArray());
	}

	public DoubleList toRadians() {
		return this.rad();
	}

	public IntList square() {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = this.value[i] * this.value[i];

		return of(a);
	}

	public DoubleList squareRoot() {
		double[] a = new double[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = Math.sqrt(this.value[i]);

		return DoubleList.of(a);
	}

	public DoubleList sqrt() {
		return this.squareRoot();
	}

	public DoubleList log10() {
		double[] a = new double[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = Math.log10(this.value[i]);

		return DoubleList.of(a);
	}

	public DoubleList log() {
		double[] a = new double[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = Math.log(this.value[i]);

		return DoubleList.of(a);
	}

	public IntList exp2() {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = (int) Math.pow(2, this.value[i]);

		return of(a);
	}

	// increment/decrement
	public IntList next() {
		int[] v = new int[this.value.length];

		for (int i = 0; i < v.length; i++)
			v[i] = this.value[i] + 1;

		return of(v);
	}

	public IntList previous() {
		int[] v = new int[this.value.length];

		for (int i = 0; i < v.length; i++)
			v[i] = this.value[i] - 1;

		return of(v);
	}

	// bitwise operators
	public IntList and(int other) {
		return this.bitwiseAnd(other);
	}

	public IntList and(IntList other) {
		return this.bitwiseAnd(other);
	}

	public IntList bitwiseAnd(int other) {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] & other;
		}

		return IntList.of(a);
	}

	public IntList bitwiseAnd(IntList other) {
		this.verifySizeMatch(this, other);

		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] & other.value[i];
		}

		return IntList.of(a);
	}

	public IntList or(int other) {
		return this.bitwiseOr(other);
	}

	public IntList or(IntList other) {
		return this.bitwiseOr(other);
	}

	public IntList bitwiseOr(int other) {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] | other;
		}

		return IntList.of(a);
	}

	public IntList bitwiseOr(IntList other) {
		this.verifySizeMatch(this, other);

		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] | other.value[i];
		}

		return IntList.of(a);
	}

	public IntList xor(int other) {
		return this.bitwiseXor(other);
	}

	public IntList xor(IntList other) {
		return this.bitwiseXor(other);
	}

	public IntList bitwiseXor(int other) {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] ^ other;
		}

		return IntList.of(a);
	}

	public IntList bitwiseXor(IntList other) {
		this.verifySizeMatch(this, other);

		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] ^ other.value[i];
		}

		return IntList.of(a);
	}

	public IntList leftShift(int other) {
		return this.bitwiseLeftShift(other);
	}

	public IntList leftShift(IntList other) {
		return this.bitwiseLeftShift(other);
	}

	public IntList bitwiseLeftShift(int other) {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] << other;
		}

		return IntList.of(a);
	}

	public IntList bitwiseLeftShift(IntList other) {
		this.verifySizeMatch(this, other);

		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] << other.value[i];
		}

		return IntList.of(a);
	}

	public IntList rightShift(int other) {
		return this.bitwiseRightShift(other);
	}

	public IntList rightShift(IntList other) {
		return this.bitwiseRightShift(other);
	}

	public IntList bitwiseRightShift(int other) {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >> other;
		}

		return IntList.of(a);
	}

	public IntList bitwiseRightShift(IntList other) {
		this.verifySizeMatch(this, other);

		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >> other.value[i];
		}

		return IntList.of(a);
	}

	public IntList bitwiseNegate() {
		int[] a = new int[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = ~this.value[i];
		}

		return IntList.of(a);
	}

	// Comparison
	public Mask eq(int other) {
		return this.equals(other);
	}

	public Mask equals(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] == other;
		}

		return Mask.of(a);
	}

	public Mask eq(IntList other) {
		return this.equals(other);
	}

	public Mask equals(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] == other.value[i];
		}

		return Mask.of(a);
	}

	public Mask lt(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] < other.value[i];
		}

		return Mask.of(a);
	}

	public Mask lt(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] < other;
		}

		return Mask.of(a);
	}

	public Mask le(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] <= other.value[i];
		}

		return Mask.of(a);
	}

	public Mask le(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] <= other;
		}

		return Mask.of(a);
	}

	public Mask gt(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] > other.value[i];
		}

		return Mask.of(a);
	}

	public Mask gt(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] > other;
		}

		return Mask.of(a);
	}

	public Mask even() {
		return this.where(i -> i.mod(2).eq(0)).mask();
	}

	public Mask odd() {
		return this.where(i -> i.mod(2).eq(1)).mask();
	}

	public Mask ge(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >= other.value[i];
		}

		return Mask.of(a);
	}

	public Mask ge(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >= other;
		}

		return Mask.of(a);
	}

	public Mask ne(IntList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] != other.value[i];
		}

		return Mask.of(a);
	}

	public Mask ne(int other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] != other;
		}

		return Mask.of(a);
	}

	// Reduction
	public OptionalInt sum() {
		if (this.value.length == 0)
			return OptionalInt.empty();

		int v = 0;
		for (int i = 0; i < this.value.length; i++)
			v += this.value[i];
		return OptionalInt.of(v);
	}

	public OptionalInt sumExact() {
		if (this.value.length == 0)
			return OptionalInt.empty();

		int v = 0;
		for (int i = 0; i < this.value.length; i++)
			v = Math.addExact(v, this.value[i]);
		return OptionalInt.of(v);
	}

	public OptionalInt product() {
		if (this.value.length == 0)
			return OptionalInt.empty();

		int v = 1;
		for (int i = 0; i < this.value.length; i++)
			v *= this.value[i];
		return OptionalInt.of(v);
	}

	public OptionalInt prod() {
		return this.product();
	}

	public OptionalInt max() {
		return IntStream.range(0, this.value.length).map(i -> this.value[i]).max();
	}

	public OptionalInt min() {
		return IntStream.range(0, this.value.length).map(i -> this.value[i]).min();
	}

	public OptionalDouble mean() {
		return IntStream.range(0, this.value.length).map(i -> this.value[i]).average();
	}

	public int getSum() {
		return this.sum().getAsInt();
	}

	public int getSumExact() {
		return this.sumExact().getAsInt();
	}

	public int getProduct() {
		return this.product().getAsInt();
	}

	public int getMax() {
		return this.max().getAsInt();
	}

	public int getMin() {
		return this.min().getAsInt();
	}

	public double getMean() {
		return this.mean().getAsDouble();
	}

	public int argmax() {
		return IntStream.range(0, this.value.length).mapToObj(i -> IntPair.of(i, this.value[i]))
				.max(IntPair.comparingByB()).map(IntPair::a).orElse(-1);
	}

	public int argmin() {
		return IntStream.range(0, this.value.length).mapToObj(i -> IntPair.of(i, this.value[i]))
				.min(IntPair.comparingByB()).map(IntPair::a).orElse(-1);
	}

	public int ptp() {
		IntSummaryStatistics stats = Arrays.stream(this.value).summaryStatistics();
		return stats.getMax() - stats.getMin();
	}

	public int peakToPeak() {
		return this.ptp();
	}

	public IntList clip(int low, int high) {
		return IntList.of(Arrays.stream(this.value).filter(i -> low <= i && high >= i).toArray());
	}

	public IntList cumSum() {
		if (0 == this.value.length) {
			return IntList.of(new int[0]);
		}

		int[] r = new int[this.value.length];

		int last = 0;

		for (int i = 0; i < this.value.length; i++) {
			last += this.value[i];
			r[i] = last;
		}

		return IntList.of(r);
	}

	public IntList cumProd() {
		if (0 == this.value.length) {
			return IntList.of(new int[0]);
		}

		int[] r = new int[this.value.length];

		int last = 1;

		for (int i = 0; i < this.value.length; i++) {
			last *= this.value[i];
			r[i] = last;
		}

		return IntList.of(r);
	}

	public double populationVar() {
		double mean = this.getMean();
		return Arrays.stream(this.value).mapToDouble(i -> Math.pow(i - mean, 2)).sum() / this.value.length;
	}

	public double sampleVar() {
		double mean = this.getMean();
		return Arrays.stream(this.value).mapToDouble(i -> Math.pow(i - mean, 2)).sum() / (this.value.length - 1.0);
	}

	public double std() {
		return Math.sqrt(this.populationVar());
	}

	public double sampleStd() {
		return Math.sqrt(this.sampleVar());
	}

	public Collection<Integer> values() {
		return Arrays.stream(value).boxed().collect(Collectors.toList());
	}

	private void verifySizeMatch(IntList left, IntList right) {
		if (left.value.length != right.value.length) {
			throw new IllegalArgumentException("array sizes don't match");
		}
	}

	// data type casting
	public LongList asLong() {
		return ListCast.toLong(this);
	}

	public DoubleList asDouble() {
		return ListCast.toDouble(this);
	}

	public DecimalList asDecimal() {
		return ListCast.toDecimal(this);
	}

	public DataList<Integer> boxed() {
		return ListCast.boxed(this);
	}

	public DataList<Integer> toInteger() {
		return this.boxed();
	}

	// implementation

	@Override
	public IntList repeat(int n) {
		int[] v = new int[n * this.value.length];

		for (int i = 0; i < n; i++) {
			System.arraycopy(this.value, 0, v, i * this.value.length, this.value.length);
		}

		return IntList.of(v);
	}

	/**
	 * Returns indices of non-zero elements
	 * 
	 * @return
	 */
	public IntList nonZero() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> this.value[i] != 0.0).toArray());
	}

	public boolean noneZero() {
		for (int i : this.value) {
			if (i == 0) {
				return false;
			}
		}

		return true;
	}

	public boolean anyNonZero() {
		for (int i : this.value) {
			if (i != 0) {
				return true;
			}
		}

		return false;
	}

	@Override
	public StringList string() {
		return StringList.of(IntStream.of(value).mapToObj(Integer::toString).collect(Collectors.toList()));
	}

	public class IndexAccessor {
		public IntList getAt(Object key) {
			return getByIndex(key);
		}

		public int getSize() {
			return index.getSize();
		}

		public Set<Object> getKeys() {
			return index.getKeys();
		}
	}

	public class RadixAccessor {
		public StringList getAt(int radix) {
			return StringList.of(IntStream.range(0, IntList.this.value.length).map(i -> IntList.this.value[i])
					.mapToObj(i -> Integer.toString(i, radix)).collect(Collectors.toList()));
		}

		public StringList base(int radix) {
			return this.getAt(radix);
		}

		public StringList getAt(IntList radix) {
			return StringList.of(IntStream.range(0, IntList.this.value.length)
					.mapToObj(i -> Integer.toString(IntList.this.value[i], radix.value[i]))
					.collect(Collectors.toList()));
		}

		public StringList base(IntList radix) {
			return this.getAt(radix);
		}

		public StringList binary() {
			return this.getAt(2);
		}

		public StringList octal() {
			return this.getAt(8);
		}

		public StringList decimal() {
			return this.getAt(10);
		}

		public StringList hex() {
			return this.getAt(16);
		}

		public StringList hexadecimal() {
			return this.hex();
		}
	}

	@Override
	public IntList asInt() {
		return this;
	}

	@Override
	public Mask asMask() {
		return Mask.of(this.size(), i -> this.value[i] != 0);
	}

	////////////// -- completing methods --

	/// methods

	// indexing

	// utilities
	// public IntMatrix toMatrix(int width) {
	// return new IntMatrix(this.value, width);
	// }

	public static IntList zip(IntList a, IntList b, IntBinaryOperator op) {
		Objects.requireNonNull(op, "operation may not be null");

		return a.zip(b, op);
	}

	/**
	 * An alias of {@link #negative()}
	 */
	public IntList negate() {
		return this.negative();
	}

	// Reduction

	private void verifySizeMatch(NambaList left, NambaList right) {
		if (left.size() != right.size()) {
			throw new IllegalArgumentException("array sizes don't match");
		}
	}

	/**
	 * Compute a decimal list containing unique values from this list.
	 * 
	 * @return A new decimal list with distinct values from this list.
	 */
	public IntList distinct() {
		return new IntList(this.stream().distinct().toArray());
	}

	/**
	 * An alias for {@link #distinct()}
	 */
	public IntList unique() {
		return this.distinct();
	}

	public IntList dropDuplicates() {
		return this.dropDuplicates(false);
	}

	public IntStream reverseStream() {
		return IntStream.iterate(this.value.length - 1, i -> i >= 0, i -> i - 1);
	}

	public IntList reversed() {
		return new IntList(reverseStream().toArray());
	}

	/**
	 * Remove duplicates from this decimal list, keeping only the first or the last
	 * value, depending on whether <code>keepLast</code> is set to
	 * <code>false</code> or <code>true</code>, respectively.
	 * 
	 * @param keepLast
	 *            If <code>true</code>, only the last instance of the duplicate is
	 *            retained.
	 * @return A new decimal list with unique values.
	 */
	public IntList dropDuplicates(boolean keepLast) {
		int[] data = (keepLast ? this.reverseStream() : this.stream()).distinct().toArray();

		if (keepLast) {
			return new IntList(data).reversed();
		} else {
			return new IntList(data);
		}
	}

	// TODO: look into storing a "sorted" flag with corresponding order.
	/**
	 * Return the given number of this list's largest values. This is equivalent to
	 * slicing a reverse-sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> largest values.
	 */
	public IntList nLargest(int n) {
		return new IntList(this.stream().sorted().skip(this.size() - n).toArray()).reversed();
	}

	/**
	 * Return the given number of this list's smallest values. This is equivalent to
	 * slicing a sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> smallest values.
	 */
	public IntList nSmallest(int n) {
		return new IntList(this.stream().sorted().limit(n).toArray());
	}

	/**
	 * Returns the number of unique elements in this list.
	 */
	public int nUnique() {
		return (int) this.stream().distinct().count();
	}

	/**
	 * Creates a copy of this list's data.
	 */
	public int[] toArray() {
		return this.stream().toArray();
	}

	/**
	 * Perform a reduction using the given binary operation.
	 * 
	 * @param reducer
	 *            The binary operation to perform the aggregation with. This may not
	 *            be null.
	 * @return The aggregated value, or null if the collection is empty.
	 */
	public OptionalInt agg(IntBinaryOperator reducer) {
		return this.stream().reduce(Objects.requireNonNull(reducer, "reducer may not be null"));
	}

	/**
	 * An alias for {@link #agg(BinaryOperator)}
	 */
	public OptionalInt aggregate(IntBinaryOperator reducer) {
		return this.agg(reducer);
	}

	/**
	 * Perform a reduction using the given binary operation. If the collection is
	 * empty, the given <code>identity</code> value is returned.
	 * 
	 * @param reducer
	 *            The binary operation to perform the aggregation with. This may not
	 *            be null.
	 * @param identity
	 *            A default value to use for the reduction.
	 * @return The aggregated value, or null if the collection is empty.
	 */
	public int agg(int identity, IntBinaryOperator reducer) {
		return this.stream().reduce(identity, Objects.requireNonNull(reducer, "reducer may not be null"));
	}

	/**
	 * An alias for {@link #agg(int, BinaryOperator)}
	 */
	public int aggregate(int identity, IntBinaryOperator reducer) {
		return this.agg(identity, reducer);
	}

	public boolean all(IntPredicate test) {
		for (int i : this.value) {
			if (!test.test(i))
				return false;
		}

		return true;
	}

	public boolean any(IntPredicate test) {
		for (int i : this.value) {
			if (test.test(i))
				return true;
		}

		return false;
	}

	// Concatenate two or more Series.
	/**
	 * Concatenate this list and <code>other</code>
	 * 
	 * @param other
	 *            List to append to this
	 * @return A new decimal list with this and <code>other</code> joined.
	 */
	public IntList concat(IntList other) {
		int[] all = new int[this.value.length + other.value.length];

		System.arraycopy(other, 0, all, this.value.length, other.value.length);

		return IntList.of(all);
	}

	/**
	 * An alias for {@link #concat(DataList)}
	 */
	public IntList append(IntList other) {
		return this.concat(other);
	}

	/**
	 * Return the integer indices that would sort the list's values.
	 */
	public IntList argSort() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> TwoInts.of(i, this.value[i]))
				.sorted(Comparator.comparing(TwoInts::b)).mapToInt(TwoInts::a).toArray());
	}

	/**
	 * Return the integer indices that would reverse-sort the list's values.
	 */
	public IntList argSortReversed() {
		// TODO: sort here? Any room for optimization?
		return this.argSort().reversed();
	}

	public IntList shift() {
		return this.shift(1);
	}

	public IntList shift(int n) {
		if (0 >= n)
			throw new IllegalArgumentException("n <= 0");

		int[] b = new int[this.value.length];

		System.arraycopy(this.value, 0, b, n, this.value.length - n);

		return IntList.of(b);
	}

	/**
	 * Returns a mask equivalent to a test for low <= x <= high for x in this list.
	 */
	public Mask between(int low, int high) {
		Objects.requireNonNull(low, "low value may not be null");
		Objects.requireNonNull(low, "high value may not be null");

		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			int v = this.value[i];
			b[i] = low <= v && high >= v;
		}

		return Mask.of(b);
	}

	/**
	 * Trim values at input thresholds. Assigns values outside boundary to boundary
	 * values.
	 * 
	 * The difference btween this method and {@link #clip(int, int)} is that this
	 * replaces values out of bounds with the closest boundary element, rather than
	 * excluding them.
	 * 
	 * @see {@link #clip(int, int)} for a similar method.
	 */
	public IntList clipToBoundaries(int low, int high) {
		int[] b = new int[this.size()];

		for (int i = 0; i < this.size(); i++) {
			int bd = this.value[i];
			b[i] = Math.max(low, Math.min(high, bd));
		}

		return of(b);
	}

	/**
	 * Apply the given <code>combiner</code> operation element-wise to this list and
	 * the given <code>other</code>.
	 * 
	 * @param other
	 *            The other decimal list to combine. This is expected to be of the
	 *            same length as this. May not be null.
	 * @param combiner
	 *            An operation to produce a new value from elements from this and
	 *            the given decimal list. May not be null.
	 * @see {@link #zip(DataList, BinaryOperator)}
	 */
	public IntList combine(IntList other, IntBinaryOperator combiner) {
		return this.zip(Objects.requireNonNull(other), Objects.requireNonNull(combiner));
	}

	/*
	 * Compare to another Series and show the differences.
	 */
	public Table compare(IntList other) {
		Mask nonEqual = this.ne(other);

		return Table.of(Arrays.asList(this.getAt(nonEqual), other.getAt(nonEqual)), null);
	}

	/**
	 * Apply a row-wise cumulative computation of values using the given function.
	 * For the first value, the function is called with null.
	 * 
	 * @param aggregator
	 *            The function computing accumulated values.
	 * @return A new IntList object with the result of that accumulation.
	 */
	public IntList cumFunc(IntBinaryOperator aggregator) {
		return this.cumFunc(aggregator, false);
	}

	/**
	 * Apply a row-wise cumulative computation of values using the given function.
	 * For the first value, the function is called with null.
	 * 
	 * @param aggregator
	 *            The function computing accumulated values.
	 * @param skipFirst
	 *            If <code>true</code>, <code>aggregator</code> is called starting
	 *            at the second row (passing first and second value of this list).
	 *            If false, the accumulation starts at the first row, but null is
	 *            used along with this list's first element to compute the first
	 *            result.
	 * @return A new IntList object with the result of that accumulation.
	 */
	public IntList cumFunc(IntBinaryOperator aggregator, boolean skipFirst) {

		if (0 == this.size())
			return IntList.of(new int[0]);

		int[] d = new int[this.size()];
		int prev;
		int offset;

		int pos = 0;

		if (skipFirst) {
			d[pos++] = 0;
			prev = this.getAt(0);
			offset = 1;
		} else {
			prev = 0;
			offset = 0;
		}

		for (int i = offset; i < this.size(); i++) {
			d[pos++] = prev = aggregator.applyAsInt(prev, this.getAt(i));
		}

		return new IntList(d);
	}

	/*
	 * count 3.0 mean 2.0 std 1.0 min 1.0 25% 1.5 50% 2.0 75% 2.5 max 3.0 dtype:
	 * float64
	 */
	// TODO: Implement
	public IntList describe() {
		return null;
	}

	/**
	 * Return the first few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public IntList head() {
		return getAt(IntRange.of(SUMMARY_SIZE));
	}

	/**
	 * Return the first <code>n</code> values of this list.
	 */
	public IntList head(int n) {
		return getAt(IntRange.of(n));
	}

	/**
	 * Return the last few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public IntList tail() {
		return getAt(IntRange.of(this.size() - SUMMARY_SIZE, this.size()));
	}

	/**
	 * Return the last <code>n</code> values of this list.
	 */
	public IntList tail(int n) {
		return getAt(IntRange.of(this.size() - n, this.size()));
	}

	public Map<Integer, Integer> histogram() {
		return this.stream().boxed().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
				.entrySet().stream().sorted(Entry.comparingByKey()).collect(Collectors.toMap(e -> e.getKey(),
						e -> e.getValue().intValue(), (a, b) -> a, LinkedHashMap::new));
	}

	public Map<Integer, Double> normalizedHistogram(boolean percentage) {
		int size = this.size();
		int factor = percentage ? 100 : 1;
		return this.stream().boxed().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
				.entrySet().stream().sorted(Entry.comparingByKey()).collect(Collectors.toMap(e -> e.getKey(),
						e -> e.getValue().doubleValue() / size * factor, (a, b) -> a, LinkedHashMap::new));
	}

	/**
	 * Returns a histogram of values in this decimal list.
	 * 
	 * @return A <code>Table</code> with values and their counts.
	 */
	public Table hist() {
		Map<Integer, Integer> groups = this.histogram();

		int[] keys = new int[groups.size()];
		int[] counts = new int[groups.size()];

		int i = 0;
		for (Entry<Integer, Integer> entry : groups.entrySet()) {
			keys[i] = entry.getKey();
			counts[i++] = entry.getValue();
		}

		return Table.of(null, IntList.of(keys), IntList.of(counts));
	}

	/**
	 * Returns a histogram of values in this decimal list, with values being the
	 * ratio of their respective counts, rather than the counts themselves. This
	 * ratio can optionally be returned as a percent value.
	 * 
	 * @param percentage
	 *            If true, values are returned as a percent rather than a raw ratio.
	 * @return A <code>Table</code> with values and their count ratios.
	 */
	public Table normalizedHist(boolean percentage) {
		Map<Integer, Double> hist = this.normalizedHistogram(percentage);

		int[] keys = new int[hist.size()];
		double[] counts = new double[hist.size()];
		int i = 0;
		for (Entry<Integer, Double> entry : hist.entrySet()) {
			keys[i] = entry.getKey();
			counts[i++] = entry.getValue();
		}

		return Table.of(null, IntList.of(keys), DoubleList.of(counts));
	}

	/*
	 * Return the row label of the maximum value.
	 * 
	 * If multiple values equal the maximum, the first row label with that value is
	 * returned.
	 */

	// TODO: Implement idxmax and idxmin
	// public Object idxmax() {
	//
	// }
	//
	// public Object idxmin() {
	//
	// }

	/**
	 * Returns true if all values in this decimal list are distinct.
	 * 
	 * @return True if the decimal list contains only unique values, false
	 *         otherwise.
	 */
	public boolean isUnique() {
		return this.distinct().size() == this.size();
	}

	/**
	 * Return the first element of the underlying data.
	 * 
	 * @return Null if the list is empty, the first element otherwise.
	 */
	public OptionalInt item() {
		return this.value.length == 0 ? OptionalInt.empty() : OptionalInt.of(this.value[0]);
	}

	/*
	 * Lazily iterate over (index, value) tuples.
	 * 
	 * This method returns an iterable tuple (index, value). This is convenient if
	 * you want to create a lazy iterator.
	 */
	// TODO: implement index-based iteration
	// public Iterator<Two<Object, int>> items() {
	//
	// }
	//
	// public Stream<Two<Object, int>> itemStream() {
	//
	// }

	/**
	 * Returns a {@link java.util.stream.Stream stream} of objects holding each
	 * element of this list and its corresponding index.
	 * 
	 * @return A stream that supplies indexed elements from this decimal list.
	 * @see {@link #indexItemStreamReversed()}
	 */
	public Stream<IndexedObject<Integer>> indexItemStream() {
		return IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value[i]));
	}

	/**
	 * Returns a {@link java.util.stream.Stream stream} of objects holding each
	 * element of this list and its corresponding index. Unlike
	 * {@link #indexItemStream()}, this method's stream will supply elements in
	 * their reverse order.
	 * 
	 * @return A stream that supplies indexed elements from this decimal list, in
	 *         reverse order.
	 * @see {@link #indexItemStream()}
	 */
	public Stream<IndexedObject<Integer>> indexItemStreamReversed() {
		int total = this.size();
		return IntStream.range(0, this.size()).map(i -> total - i - 1)
				.mapToObj(i -> IndexedObject.of(i, this.value[i]));
	}

	/*
	 * Return unbiased kurtosis over requested axis.
	 * 
	 * Kurtosis obtained using Fisher’s definition of kurtosis (kurtosis o
	 */
	// TODO: Implement kurtosis
	// public int kurtosis() {
	//
	// }
	//
	// public int kurt() {
	//
	// }

	/*
	 * Return the mean absolute deviation of the values for the requested axis.
	 */
	// TODO: Implement this
	// public IntList meanAbsoluteDeviation() {
	//
	// }
	// public IntList mad() {
	// return this.meanAbsoluteDeviation();
	// }

	/**
	 * Replaces with the given value where the predicate evaluates to true.
	 * 
	 * @param cond
	 *            The condition to test with.
	 * @param val
	 *            The value to replace matching elements.
	 * @return A new list with matching elements replaced.
	 */
	public IntList replaceWhere(IntPredicate cond, int val) {
		Objects.requireNonNull(cond);

		int[] v = new int[this.size()];

		for (int i = 0; i < this.size(); i++) {
			int value = this.getAt(i);
			if (cond.test(value)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return IntList.of(v);
	}

	/**
	 * Replaces with the given value where the mask is set to true.
	 * 
	 * @param cond
	 *            The mask with indexes to replace set to true.
	 * @param val
	 *            The value to replace matching elements.
	 * @return A new list with matching elements replaced.
	 */
	public IntList replaceWhere(Mask cond, int val) {
		Objects.requireNonNull(cond);

		int[] v = new int[this.size()];

		for (int i = 0; i < this.size(); i++) {
			int value = this.getAt(i);
			if (cond.getAt(i)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return IntList.of(v);
	}

	/**
	 * Return the median of the values. Note that this skips null values.
	 */
	public OptionalInt median() {

		IntList withoutNa = this.sorted();

		if (withoutNa.size() == 0)
			return OptionalInt.empty();
		if (withoutNa.size() % 2 == 1) {
			return withoutNa.getAt(withoutNa.size() / 2 + 1);
		} else {
			int medLocation = withoutNa.size() / 2;
			return OptionalInt.of((withoutNa.value[medLocation] + withoutNa.value[medLocation + 1]) / 2);
		}
	}

	/*
	 * Return value at the given quantile.
	 */
	// TODO: implement quantile
	// public int quantile(int quantile) {
	/*
	 * This optional parameter specifies the interpolation method to use, when the
	 * desired quantile lies between two data points i and j:
	 * 
	 * linear: i + (j - i) * fraction, where fraction is the fractional part of the
	 * index surrounded by i and j.
	 * 
	 * lower: i.
	 * 
	 * higher: j.
	 * 
	 * nearest: i or j whichever is nearest.
	 * 
	 * midpoint: (i + j) / 2.
	 */
	// }

	/*
	 * Compute numerical data ranks (1 through n) along axis.
	 * 
	 * By default, equal values are assigned a rank that is the average of the ranks
	 * of those values.
	 */
	// TODO: Implement rank()
	// public IntList rank() {
	/*
	 * How to rank the group of records that have the same value (i.e. ties):
	 * 
	 * average: average rank of the group
	 * 
	 * min: lowest rank in the group
	 * 
	 * max: highest rank in the group
	 * 
	 * first: ranks assigned in order they appear in the array
	 * 
	 * dense: like ‘min’, but rank always increases by 1 between groups.
	 */
	// }

	/*
	 * Needs to be defined with window objects.
	 */
	// TODO: Implement Rolling
	// public Map<Two<int, int>, IntList> rolling() {
	//
	// }

	/**
	 * Extract a sample of values from this decimal list.
	 * 
	 * @param fraction
	 *            The fraction of the size of this list to sample. Must be a valid
	 *            ratio: 0.0 < sample < 1.0
	 * @return A new list with a sample from this list.
	 */
	public IntList sample(double fraction) {
		if (!(0 < fraction && fraction < 1))
			throw new IllegalArgumentException("fraction must be greater than 0 and smaller than 1");
		return this.sample((int) (this.size() * fraction));
	}

	/**
	 * Extract a sample of values from this decimal list.
	 * 
	 * @param size
	 *            The size of the sample.
	 * @return A new decimal list with sample values drawn from this list.
	 */
	public IntList sample(int size) {
		return this.getAt(IntData.instance().randomArray(size, 0, this.size()));
	}

	public IntList sample(int size, long randomState) {
		return this.getAt(IntData.instance().randomArray(randomState, size, 0, this.size()));
	}

	public IntList sorted(boolean descending) {
		IntList v = of(this.stream().sorted().toArray());
		if (descending) {
			return v.reversed();
		} else {
			return v;
		}
	}

	public IntList sorted() {
		return this.sorted(false);
	}

	public IntList where(Mask mask) {
		return this.getAt(mask);
	}

	public <K> IntGrouping<K> groupBy(IntFunction<K> classifier) {
		return IntGrouping.of(this.boxed(),
				IntStream.range(0, size()).mapToObj(i -> Two.of(i, classifier.apply(this.value[i])))
						.collect(Collectors.groupingBy(Two::b, Collectors.mapping(Two::a, Collectors.toList()))));
	}

	public StringList string(DecimalFormat numberFormat) {
		return StringList.of(this.stream().mapToObj(numberFormat::format).collect(Collectors.toList()));
	}

	public Mask test(IntPredicate predicate) {
		Objects.requireNonNull(predicate, "predicate cannot be null");

		boolean[] b = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++)
			b[i] = predicate.test(this.value[i]);

		return Mask.of(b);
	}

	// Cookbook
	/**
	 * Returns a pair with this list partitioned in two lists: the first one
	 * contains values that passed the given predicate's test, and the second one,
	 * values that failed the test.
	 * 
	 * @param predicate
	 *            The predicate to partition values by.
	 * @return A pair where Two.a contains true values and Two.b false values.
	 */
	public Two<IntList, IntList> partition(IntPredicate predicate) {
		Objects.requireNonNull(predicate, "predicate may not be null");

		return this.test(predicate).partition(this, l -> IntList.of(l.stream().mapToInt(Integer::intValue).toArray()));
	}

	public Two<IntList, IntList> partition(Mask mask) {
		return Objects.requireNonNull(mask, "mask may not be null").partition(this,
				l -> IntList.of(l.stream().mapToInt(Integer::intValue).toArray()));
	}

	public IntList applyWithIndex(IntBinaryOperator op) {
		Objects.requireNonNull(op, "operation cannot be null");

		int[] b = new int[this.size()];

		for (int i = 0; i < size(); i++) {
			b[i] = op.applyAsInt(i, this.value[i]);
		}

		return of(b);
	}

	public <T> DataList<T> applyWithIndex(BiFunction<Integer, Integer, T> op) {
		Objects.requireNonNull(op, "operation cannot be null");

		List<T> res = new ArrayList<>(this.size());

		for (int i = 0; i < size(); i++) {
			res.add(op.apply(i, this.value[i]));
		}

		return new DataList<>(DataType.OBJECT, res);
	}

	public IntList applyWhere(Mask mask, int newVal) {
		// explicit parameter types required to resolve ambiguity
		return this.applyWithIndex((int i, int val) -> mask.getAt(i) ? newVal : val);
	}

	public IntList applyWhere(Mask mask, IntBinaryOperator mapper) {
		return this.applyWithIndex((int i, int val) -> mask.getAt(i) ? mapper.applyAsInt(i, val) : val);
	}

	public IntList applyWhere(Mask mask, int trueVal, int falseVal) {
		return this.applyWithIndex((int i, int val) -> mask.getAt(i) ? trueVal : falseVal);
	}

	public IntList putAt(Mask mask, TwoInts values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(mask, values.a(), values.b());
	}

	public IntList putAt(Mask mask, int newVal) {
		return this.applyWhere(mask, newVal);
	}

	public IntList applyWhere(IntPredicate test, int newVal) {
		return this.apply(val -> test.test(val) ? newVal : null);
	}

	public IntList putAt(IntPredicate test, int newVal) {
		return this.applyWhere(test, newVal);
	}

	public IntList applyWhere(IntPredicate test, int trueValue, int falseValue) {
		return this.apply(val -> test.test(val) ? trueValue : falseValue);
	}

	public IntList putAt(IntPredicate test, TwoInts values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(test, values.a(), values.b());
	}

	public IntList applyWhere(IntPredicate test, IntUnaryOperator valueMapper) {
		return this.test(test).applyWhereTrue(i -> valueMapper.applyAsInt(this.getAt(i)),
				l -> IntList.of(l.stream().mapToInt(Integer::intValue).toArray()));
	}

	public IntList getAt(IntPredicate predicate) {
		return this.getAt(this.test(predicate));
	}

	public IntList getAt(int from, int to) {
		return this.getAt(IntRange.of(from, to));
	}

	public IntList putAt(IntPredicate test, IntUnaryOperator valueMapper) {
		return this.applyWhere(test, valueMapper);
	}

	///////////// -- end completing methods --
}
