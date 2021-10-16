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

import java.util.Arrays;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntFunction;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.namba.Namba;
import io.namba.arrays.data.IntPair;
import io.namba.arrays.data.tuple.Two;
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
		var array = new IntList(size);
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
		var operation = Objects.requireNonNull(op);
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

	public static void main(String[] args) {
		Namba nb = Namba.instance();

		IntList rr = nb.data.ints.range(1, 100).repeat(2);

		long s = System.currentTimeMillis();
		System.out.println(rr.where(i -> i.lt(17)).gt(12).odd().list());

		System.out.println(nb.data.doubles.linSpace(4, 2, 4).asDecimal());
	}
}
