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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.LongBinaryOperator;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import io.namba.arrays.agg.LongGrouping;
import io.namba.arrays.data.IndexedLong;
import io.namba.arrays.data.IntData;
import io.namba.arrays.data.tuple.Two;
import io.namba.arrays.data.tuple.TwoInts;
import io.namba.arrays.range.IntRange;
import io.namba.functions.IndexedLongFunction;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class LongList implements NambaList {

	protected final long[] value;

	private LongList(long[] a) {
		this.value = a;
	}

	public static LongList of(long[] a) {
		return new LongList(a);
	}

	public LongStream stream() {
		return Arrays.stream(this.value);
	}

	public DecimalList asDecimal() {
		return ListCast.toDecimal(this);
	}

	public DoubleList asDouble() {
		return ListCast.toDouble(this);
	}

	public IntList asInt() {
		return ListCast.toInt(this);
	}

	/////// from intlist
	// accessors
	private LongList getAt(IntStream stream) {
		return LongList.of(Objects.requireNonNull(stream).filter(i -> i >= 0 && i < this.value.length)
				.mapToLong(i -> this.value[i]).toArray());
	}

	public LongList getAt(Mask mask) {
		return this.getAt(mask.truthy().value);
	}

	@Override
	public LongList getAt(int[] is) {
		return getAt(Arrays.stream(is));
	}

	// TODO: implement LongRef and .where here
	public Mask where(LongPredicate p) {
		boolean[] b = new boolean[this.size()];
		for (int i = 0; i < this.size(); i++) {
			b[i] = p.test(this.value[i]);
		}
		return Mask.of(b);
	}

	public LongList take(int size) {
		return getAt(IntRange.of(size));
	}

	@SuppressWarnings("unchecked")
	public <T> T getAt(int loc) {
		return (T) Long.valueOf(this.value[loc]);
	}

	public LongList getAt(IntRange range) {
		return getAt(range.stream());
	}

	public LongList getAt(List<Integer> range) {
		return getAt(range.stream().mapToInt(Integer::intValue));
	}

	public Stream<Two<Integer, Long>> indexedStream() {
		return IntStream.range(0, size()).mapToObj(i -> Two.of(i, this.getAt(i)));
	}

	// operations
	public LongList zip(LongList other, LongBinaryOperator op) {
		this.verifySizeMatch(this, other);

		long[] res = new long[this.value.length];
		for (int i = 0; i < res.length; i++) {
			res[i] = op.applyAsLong(this.value[i], other.value[i]);
		}

		return of(res);
	}

	public LongList map(LongUnaryOperator op) {
		LongUnaryOperator operation = Objects.requireNonNull(op);
		long[] n = new long[this.value.length];
		for (int i = 0; i < this.value.length; i++) {
			n[i] = operation.applyAsLong(this.value[i]);
		}
		return LongList.of(n);
	}

	public LongList apply(LongUnaryOperator op) {
		return this.map(op);
	}

	public LongList multiply(long n) {
		return this.map(i -> i * n);
	}

	public LongList multiply(LongList n) {
		return this.zip(n, (a, b) -> a * b);
	}

	public LongList minus(long n) {
		return this.map(i -> i - n);
	}

	public LongList minus(LongList n) {
		return this.zip(n, (a, b) -> a - b);
	}

	public LongList plus(long n) {
		return this.map(i -> i + n);
	}

	public LongList plus(LongList n) {
		return this.zip(n, (a, b) -> a + b);
	}

	public LongList divide(long n) {
		return this.map(i -> i / n);
	}

	public LongList divide(LongList n) {
		return this.zip(n, (a, b) -> a / b);
	}

	public LongList power(long n) {
		return this.map(i -> (long) Math.pow(i, n));
	}

	public LongList power(LongList n) {
		return this.zip(n, (a, b) -> (long) Math.pow(a, b));
	}

	public LongList abs() {
		long[] r = new long[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = Math.abs(this.value[i]);

		return of(r);
	}

	public LongList absolute() {
		return this.abs();
	}

	public LongList negative() {
		long[] r = new long[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = -this.value[i];

		return of(r);
	}

	public LongList positive() {
		return this;
	}

	public LongList mod(long other) {
		long[] r = new long[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = this.value[i] % other;

		return of(r);
	}

	public LongList mod(LongList n) {
		return this.zip(n, (a, b) -> a % b);
	}

	public LongList signum() {
		long[] r = new long[this.value.length];

		for (int i = 0; i < r.length; i++)
			r[i] = Long.signum(this.value[i]);

		return of(r);
	}

	public LongList sign() {
		return this.signum();
	}

	public long mode() {
		return IntStream.range(0, this.value.length).mapToObj(i -> this.value[i])
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet().stream()
				.max(Map.Entry.comparingByValue()).orElseThrow(() -> new IllegalStateException("no value found"))
				.getKey();
	}

	public DoubleList sin() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::sin).toArray());
	}

	public DoubleList cos() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::cos).toArray());
	}

	public DoubleList tan() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::tan).toArray());
	}

	public DoubleList arcsin() {
		return this.asin();
	}

	public DoubleList asin() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::asin).toArray());
	}

	public DoubleList arccos() {
		return this.acos();
	}

	public DoubleList acos() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::acos).toArray());
	}

	public DoubleList arctan() {
		return this.acos();
	}

	public DoubleList atan() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::atan).toArray());
	}

	public DoubleList hsin() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::sinh).toArray());
	}

	public DoubleList sinh() {
		return this.hsin();
	}

	public DoubleList hcos() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::cosh).toArray());
	}

	public DoubleList cosh() {
		return this.hcos();
	}

	public DoubleList htan() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::tanh).toArray());
	}

	public DoubleList tanh() {
		return this.htan();
	}

	public DoubleList rad() {
		return DoubleList.of(LongStream.of(this.value).mapToDouble(Math::toRadians).toArray());
	}

	public DoubleList toRadians() {
		return this.rad();
	}

	public LongList square() {
		long[] a = new long[this.value.length];

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

	public LongList exp2() {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++)
			a[i] = (long) Math.pow(2, this.value[i]);

		return of(a);
	}

	// increment/decrement
	public LongList next() {
		long[] v = new long[this.value.length];

		for (int i = 0; i < v.length; i++)
			v[i] = this.value[i] + 1;

		return of(v);
	}

	public LongList previous() {
		long[] v = new long[this.value.length];

		for (int i = 0; i < v.length; i++)
			v[i] = this.value[i] - 1;

		return of(v);
	}

	// bitwise operators
	public LongList and(long other) {
		return this.bitwiseAnd(other);
	}

	public LongList and(LongList other) {
		return this.bitwiseAnd(other);
	}

	public LongList bitwiseAnd(long other) {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] & other;
		}

		return LongList.of(a);
	}

	public LongList bitwiseAnd(LongList other) {
		this.verifySizeMatch(this, other);

		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] & other.value[i];
		}

		return LongList.of(a);
	}

	public LongList or(long other) {
		return this.bitwiseOr(other);
	}

	public LongList or(LongList other) {
		return this.bitwiseOr(other);
	}

	public LongList bitwiseOr(long other) {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] | other;
		}

		return LongList.of(a);
	}

	public LongList bitwiseOr(LongList other) {
		this.verifySizeMatch(this, other);

		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] | other.value[i];
		}

		return LongList.of(a);
	}

	public LongList xor(long other) {
		return this.bitwiseXor(other);
	}

	public LongList xor(LongList other) {
		return this.bitwiseXor(other);
	}

	public LongList bitwiseXor(long other) {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] ^ other;
		}

		return LongList.of(a);
	}

	public LongList bitwiseXor(LongList other) {
		this.verifySizeMatch(this, other);

		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] ^ other.value[i];
		}

		return LongList.of(a);
	}

	public LongList leftShift(long other) {
		return this.bitwiseLeftShift(other);
	}

	public LongList leftShift(LongList other) {
		return this.bitwiseLeftShift(other);
	}

	public LongList bitwiseLeftShift(long other) {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] << other;
		}

		return LongList.of(a);
	}

	public LongList bitwiseLeftShift(LongList other) {
		this.verifySizeMatch(this, other);

		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] << other.value[i];
		}

		return LongList.of(a);
	}

	public LongList rightShift(long other) {
		return this.bitwiseRightShift(other);
	}

	public LongList rightShift(LongList other) {
		return this.bitwiseRightShift(other);
	}

	public LongList bitwiseRightShift(long other) {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >> other;
		}

		return LongList.of(a);
	}

	public LongList bitwiseRightShift(LongList other) {
		this.verifySizeMatch(this, other);

		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >> other.value[i];
		}

		return LongList.of(a);
	}

	public LongList bitwiseNegate() {
		long[] a = new long[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = ~this.value[i];
		}

		return LongList.of(a);
	}

	// Comparison
	public Mask eq(long other) {
		return this.equals(other);
	}

	public Mask equals(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] == other;
		}

		return Mask.of(a);
	}

	public Mask eq(LongList other) {
		return this.equals(other);
	}

	public Mask equals(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] == other.value[i];
		}

		return Mask.of(a);
	}

	public Mask lt(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] < other.value[i];
		}

		return Mask.of(a);
	}

	public Mask lt(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] < other;
		}

		return Mask.of(a);
	}

	public Mask le(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] <= other.value[i];
		}

		return Mask.of(a);
	}

	public Mask le(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] <= other;
		}

		return Mask.of(a);
	}

	public Mask gt(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] > other.value[i];
		}

		return Mask.of(a);
	}

	public Mask gt(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] > other;
		}

		return Mask.of(a);
	}

	public Mask even() {
		return this.where(i -> 0 == i % 2);
	}

	public Mask odd() {
		return this.where(i -> 1 == i % 2);
	}

	public Mask ge(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >= other.value[i];
		}

		return Mask.of(a);
	}

	public Mask ge(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] >= other;
		}

		return Mask.of(a);
	}

	public Mask ne(LongList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] != other.value[i];
		}

		return Mask.of(a);
	}

	public Mask ne(long other) {
		boolean[] a = new boolean[this.value.length];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value[i] != other;
		}

		return Mask.of(a);
	}

	// Reduction
	public OptionalLong sum() {
		if (this.value.length == 0)
			return OptionalLong.empty();

		long v = 0;
		for (int i = 0; i < this.value.length; i++)
			v += this.value[i];
		return OptionalLong.of(v);
	}

	public OptionalLong sumExact() {
		if (this.value.length == 0)
			return OptionalLong.empty();

		long v = 0;
		for (int i = 0; i < this.value.length; i++)
			v = Math.addExact(v, this.value[i]);
		return OptionalLong.of(v);
	}

	public OptionalLong product() {
		if (this.value.length == 0)
			return OptionalLong.empty();

		long v = 1;
		for (int i = 0; i < this.value.length; i++)
			v *= this.value[i];
		return OptionalLong.of(v);
	}

	public OptionalLong prod() {
		return this.product();
	}

	public OptionalLong max() {
		return Arrays.stream(this.value).max();
	}

	public OptionalLong min() {
		return Arrays.stream(this.value).min();
	}

	public OptionalDouble mean() {
		return Arrays.stream(this.value).average();
	}

	public long getSum() {
		return this.sum().getAsLong();
	}

	public long getSumExact() {
		return this.sumExact().getAsLong();
	}

	public long getProduct() {
		return this.product().getAsLong();
	}

	public long getMax() {
		return this.max().getAsLong();
	}

	public long getMin() {
		return this.min().getAsLong();
	}

	public double getMean() {
		return this.mean().getAsDouble();
	}

	public int argmax() {
		return IntStream.range(0, this.value.length).mapToObj(i -> IndexedLong.of(i, this.value[i]))
				.max(Comparator.comparingLong(IndexedLong::value)).map(IndexedLong::index).orElse(-1);
	}

	public int argmin() {
		return IntStream.range(0, this.value.length).mapToObj(i -> IndexedLong.of(i, this.value[i]))
				.min(Comparator.comparingLong(IndexedLong::value)).map(IndexedLong::index).orElse(-1);
	}

	public long ptp() {
		LongSummaryStatistics stats = Arrays.stream(this.value).summaryStatistics();
		return stats.getMax() - stats.getMin();
	}

	public long peakToPeak() {
		return this.ptp();
	}

	public LongList clip(long low, long high) {
		return LongList.of(Arrays.stream(this.value).filter(i -> low <= i && high >= i).toArray());
	}

	public LongList cumSum() {
		if (0 == this.value.length) {
			return LongList.of(new long[0]);
		}

		long[] r = new long[this.value.length];

		long last = 0;

		for (int i = 0; i < this.value.length; i++) {
			last += this.value[i];
			r[i] = last;
		}

		return LongList.of(r);
	}

	public LongList cumProd() {
		if (0 == this.value.length) {
			return LongList.of(new long[0]);
		}

		long[] r = new long[this.value.length];

		long last = 1;

		for (int i = 0; i < this.value.length; i++) {
			last *= this.value[i];
			r[i] = last;
		}

		return LongList.of(r);
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

	public Collection<Long> values() {
		return Arrays.stream(value).boxed().collect(Collectors.toList());
	}

	private void verifySizeMatch(LongList left, LongList right) {
		if (left.value.length != right.value.length) {
			throw new IllegalArgumentException("array sizes don't match");
		}
	}

	// data type casting
	public DataList<Long> toLong() {
		return this.boxed();
	}

	// implementation

	@Override
	public LongList repeat(int n) {
		long[] v = new long[n * this.value.length];

		for (int i = 0; i < n; i++) {
			System.arraycopy(this.value, 0, v, i * this.value.length, this.value.length);
		}

		return LongList.of(v);
	}

	/**
	 * Returns indices of non-zero elements
	 * 
	 * @return
	 */
	public LongList nonZero() {
		return new LongList(IntStream.range(0, this.value.length).filter(i -> this.value[i] != 0.0)
				.mapToLong(i -> this.value[i]).toArray());
	}

	public boolean noneZero() {
		for (long i : this.value) {
			if (i == 0) {
				return false;
			}
		}

		return true;
	}

	public boolean anyNonZero() {
		for (long i : this.value) {
			if (i != 0) {
				return true;
			}
		}

		return false;
	}

	@Override
	public StringList string() {
		return StringList.of(LongStream.of(value).mapToObj(Long::toString).collect(Collectors.toList()));
	}

	@Override
	public Index getIndex() {
		// return index;
		throw new UnsupportedOperationException("Indexing not supported for long");
	}

	@Override
	public Mask asMask() {
		return Mask.of(this.size(), i -> this.value[i] != 0);
	}

	////////////// -- completing methods --

	public static LongList zip(LongList a, LongList b, LongBinaryOperator op) {
		Objects.requireNonNull(op, "operation may not be null");

		return a.zip(b, op);
	}

	/**
	 * An alias of {@link #negative()}
	 */
	public LongList negate() {
		return this.negative();
	}

	// Reduction

	/**
	 * Compute a decimal list containing unique values from this list.
	 * 
	 * @return A new decimal list with distinct values from this list.
	 */
	public LongList distinct() {
		return new LongList(this.stream().distinct().toArray());
	}

	/**
	 * An alias for {@link #distinct()}
	 */
	public LongList unique() {
		return this.distinct();
	}

	public LongList dropDuplicates() {
		return this.dropDuplicates(false);
	}

	public LongStream reverseStream() {
		return LongStream.iterate(this.value.length - 1, i -> i - 1).limit(this.size());
	}

	public LongList reversed() {
		return new LongList(reverseStream().toArray());
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
	public LongList dropDuplicates(boolean keepLast) {
		long[] data = (keepLast ? this.reverseStream() : this.stream()).distinct().toArray();

		if (keepLast) {
			return new LongList(data).reversed();
		} else {
			return new LongList(data);
		}
	}

	// TODO: look longo storing a "sorted" flag with corresponding order.
	/**
	 * Return the given number of this list's largest values. This is equivalent to
	 * slicing a reverse-sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> largest values.
	 */
	public LongList nLargest(long n) {
		return new LongList(this.stream().sorted().skip(this.size() - n).toArray()).reversed();
	}

	/**
	 * Return the given number of this list's smallest values. This is equivalent to
	 * slicing a sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> smallest values.
	 */
	public LongList nSmallest(long n) {
		return new LongList(this.stream().sorted().limit(n).toArray());
	}

	/**
	 * Returns the number of unique elements in this list.
	 */
	public long nUnique() {
		return (long) this.stream().distinct().count();
	}

	/**
	 * Creates a copy of this list's data.
	 */
	public long[] toArray() {
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
	public OptionalLong agg(LongBinaryOperator reducer) {
		return this.stream().reduce(Objects.requireNonNull(reducer, "reducer may not be null"));
	}

	/**
	 * An alias for {@link #agg(BinaryOperator)}
	 */
	public OptionalLong aggregate(LongBinaryOperator reducer) {
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
	public long agg(long identity, LongBinaryOperator reducer) {
		return this.stream().reduce(identity, Objects.requireNonNull(reducer, "reducer may not be null"));
	}

	/**
	 * An alias for {@link #agg(long, BinaryOperator)}
	 */
	public long aggregate(long identity, LongBinaryOperator reducer) {
		return this.agg(identity, reducer);
	}

	public boolean all(LongPredicate test) {
		for (long i : this.value) {
			if (!test.test(i))
				return false;
		}

		return true;
	}

	public boolean any(LongPredicate test) {
		for (long i : this.value) {
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
	public LongList concat(LongList other) {
		long[] all = new long[this.value.length + other.value.length];

		System.arraycopy(other, 0, all, this.value.length, other.value.length);

		return LongList.of(all);
	}

	/**
	 * An alias for {@link #concat(DataList)}
	 */
	public LongList append(LongList other) {
		return this.concat(other);
	}

	/**
	 * Return the integer indices that would sort the list's values.
	 */
	public IntList argSort() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> IndexedLong.of(i, this.value[i]))
				.sorted(Comparator.comparing(IndexedLong::value)).mapToInt(IndexedLong::index).toArray());
	}

	/**
	 * Return the integer indices that would reverse-sort the list's values.
	 */
	public IntList argSortReversed() {
		// TODO: sort here? Any room for optimization?
		return this.argSort().reversed();
	}

	public LongList shift() {
		return this.shift(1);
	}

	public LongList shift(int n) {
		if (0 >= n)
			throw new IllegalArgumentException("n <= 0");

		long[] b = new long[this.value.length];

		System.arraycopy(this.value, 0, b, n, this.value.length - n);

		return LongList.of(b);
	}

	/**
	 * Returns a mask equivalent to a test for low <= x <= high for x in this list.
	 */
	public Mask between(long low, long high) {
		Objects.requireNonNull(low, "low value may not be null");
		Objects.requireNonNull(low, "high value may not be null");

		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			long v = this.value[i];
			b[i] = low <= v && high >= v;
		}

		return Mask.of(b);
	}

	/**
	 * Trim values at input thresholds. Assigns values outside boundary to boundary
	 * values.
	 * 
	 * The difference btween this method and {@link #clip(long, long)} is that this
	 * replaces values out of bounds with the closest boundary element, rather than
	 * excluding them.
	 * 
	 * @see {@link #clip(long, long)} for a similar method.
	 */
	public LongList clipToBoundaries(long low, long high) {
		long[] b = new long[this.size()];

		for (int i = 0; i < this.size(); i++) {
			long bd = this.value[i];
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
	public LongList combine(LongList other, LongBinaryOperator combiner) {
		return this.zip(Objects.requireNonNull(other), Objects.requireNonNull(combiner));
	}

	/*
	 * Compare to another Series and show the differences.
	 */
	public Table compare(LongList other) {
		Mask nonEqual = this.ne(other);

		return Table.of(Arrays.asList(this.getAt(nonEqual), other.getAt(nonEqual)), null);
	}

	/**
	 * Apply a row-wise cumulative computation of values using the given function.
	 * For the first value, the function is called with null.
	 * 
	 * @param aggregator
	 *            The function computing accumulated values.
	 * @return A new LongList object with the result of that accumulation.
	 */
	public LongList cumFunc(LongBinaryOperator aggregator) {
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
	 * @return A new LongList object with the result of that accumulation.
	 */
	public LongList cumFunc(LongBinaryOperator aggregator, boolean skipFirst) {

		if (0 == this.size())
			return LongList.of(new long[0]);

		long[] d = new long[this.size()];
		long prev;
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
			d[pos++] = prev = aggregator.applyAsLong(prev, this.getAt(i));
		}

		return new LongList(d);
	}

	/*
	 * count 3.0 mean 2.0 std 1.0 min 1.0 25% 1.5 50% 2.0 75% 2.5 max 3.0 dtype:
	 * float64
	 */
	// TODO: Implement
	public LongList describe() {
		return null;
	}

	/**
	 * Return the first few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public LongList head() {
		return getAt(IntRange.of(SUMMARY_SIZE));
	}

	/**
	 * Return the first <code>n</code> values of this list.
	 */
	public LongList head(int n) {
		return getAt(IntRange.of(n));
	}

	/**
	 * Return the last few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public LongList tail() {
		return getAt(IntRange.of(this.size() - SUMMARY_SIZE, this.size()));
	}

	/**
	 * Return the last <code>n</code> values of this list.
	 */
	public LongList tail(int n) {
		return getAt(IntRange.of(this.size() - n, this.size()));
	}

	public Map<Long, Integer> histogram() {
		return this.stream().boxed().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
				.entrySet().stream().sorted(Entry.comparingByKey()).collect(Collectors.toMap(e -> e.getKey(),
						e -> e.getValue().intValue(), (a, b) -> a, LinkedHashMap::new));
	}

	public Map<Long, Double> normalizedHistogram(boolean percentage) {
		long size = this.size();
		long factor = percentage ? 100 : 1;
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
		Map<Long, Integer> groups = this.histogram();

		long[] keys = new long[groups.size()];
		long[] counts = new long[groups.size()];

		int i = 0;
		for (Entry<Long, Integer> entry : groups.entrySet()) {
			keys[i] = entry.getKey();
			counts[i++] = entry.getValue();
		}

		return Table.of(null, LongList.of(keys), LongList.of(counts));
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
		Map<Long, Double> hist = this.normalizedHistogram(percentage);

		long[] keys = new long[hist.size()];
		double[] counts = new double[hist.size()];
		int i = 0;
		for (Entry<Long, Double> entry : hist.entrySet()) {
			keys[i] = entry.getKey();
			counts[i++] = entry.getValue();
		}

		return Table.of(null, LongList.of(keys), DoubleList.of(counts));
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
	public OptionalLong item() {
		return this.value.length == 0 ? OptionalLong.empty() : OptionalLong.of(this.value[0]);
	}

	/**
	 * Returns a {@link java.util.stream.Stream stream} of objects holding each
	 * element of this list and its corresponding index.
	 * 
	 * @return A stream that supplies indexed elements from this decimal list.
	 * @see {@link #indexItemStreamReversed()}
	 */
	public Stream<IndexedLong> indexItemStream() {
		return IntStream.range(0, this.size()).mapToObj(i -> IndexedLong.of(i, this.value[i]));
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
	public Stream<IndexedLong> indexItemStreamReversed() {
		int total = this.size();
		return IntStream.range(0, this.size()).map(i -> total - i - 1).mapToObj(i -> IndexedLong.of(i, this.value[i]));
	}

	/**
	 * Replaces with the given value where the predicate evaluates to true.
	 * 
	 * @param cond
	 *            The condition to test with.
	 * @param val
	 *            The value to replace matching elements.
	 * @return A new list with matching elements replaced.
	 */
	public LongList replaceWhere(LongPredicate cond, long val) {
		Objects.requireNonNull(cond);

		long[] v = new long[this.size()];

		for (int i = 0; i < this.size(); i++) {
			long value = this.getAt(i);
			if (cond.test(value)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return LongList.of(v);
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
	public LongList replaceWhere(Mask cond, long val) {
		Objects.requireNonNull(cond);

		long[] v = new long[this.size()];

		for (int i = 0; i < this.size(); i++) {
			long value = this.getAt(i);
			if (cond.getAt(i)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return LongList.of(v);
	}

	/**
	 * Return the median of the values. Note that this skips null values.
	 */
	public OptionalLong median() {

		LongList withoutNa = this.sorted();

		if (withoutNa.size() == 0)
			return OptionalLong.empty();
		if (withoutNa.size() % 2 == 1) {
			return withoutNa.getAt(withoutNa.size() / 2 + 1);
		} else {
			int medLocation = withoutNa.size() / 2;
			return OptionalLong.of((withoutNa.value[medLocation] + withoutNa.value[medLocation + 1]) / 2);
		}
	}

	/**
	 * Extract a sample of values from this decimal list.
	 * 
	 * @param fraction
	 *            The fraction of the size of this list to sample. Must be a valid
	 *            ratio: 0.0 < sample < 1.0
	 * @return A new list with a sample from this list.
	 */
	public LongList sample(double fraction) {
		if (!(0 < fraction && fraction < 1))
			throw new IllegalArgumentException("fraction must be greater than 0 and smaller than 1");
		return this.sample((long) (this.size() * fraction));
	}

	/**
	 * Extract a sample of values from this decimal list.
	 * 
	 * @param size
	 *            The size of the sample.
	 * @return A new decimal list with sample values drawn from this list.
	 */
	public LongList sample(int size) {
		return this.getAt(IntData.instance().randomArray(size, 0, this.size()));
	}

	public LongList sample(int size, long randomState) {
		return this.getAt(IntData.instance().randomArray(randomState, size, 0, this.size()));
	}

	public LongList sorted(boolean descending) {
		LongList v = of(this.stream().sorted().toArray());
		if (descending) {
			return v.reversed();
		} else {
			return v;
		}
	}

	public LongList sorted() {
		return this.sorted(false);
	}

	public LongList where(Mask mask) {
		return this.getAt(mask);
	}

	public DataList<Long> boxed() {
		return ListCast.boxed(this);
	}

	public <K> LongGrouping<K> groupBy(LongFunction<K> classifier) {
		return LongGrouping.of(this.boxed(),
				IntStream.range(0, size()).mapToObj(i -> Two.of(i, classifier.apply(this.value[i])))
						.collect(Collectors.groupingBy(Two::b, Collectors.mapping(Two::a, Collectors.toList()))));
	}

	public StringList string(DecimalFormat numberFormat) {
		return StringList.of(this.stream().mapToObj(numberFormat::format).collect(Collectors.toList()));
	}

	public Mask test(LongPredicate predicate) {
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
	public Two<LongList, LongList> partition(LongPredicate predicate) {
		Objects.requireNonNull(predicate, "predicate may not be null");

		return this.test(predicate).partition(this, l -> LongList.of(l.stream().mapToLong(Long::longValue).toArray()));
	}

	public Two<LongList, LongList> partition(Mask mask) {
		return Objects.requireNonNull(mask, "mask may not be null").partition(this,
				l -> LongList.of(l.stream().mapToLong(Long::longValue).toArray()));
	}

	public LongList applyWithIndex(IndexedLongFunction op) {
		Objects.requireNonNull(op, "operation cannot be null");

		long[] b = new long[this.size()];

		for (int i = 0; i < size(); i++) {
			b[i] = op.apply(i, this.value[i]);
		}

		return of(b);
	}

	public <T> DataList<T> applyWithIndex(BiFunction<Integer, Long, T> op) {
		Objects.requireNonNull(op, "operation cannot be null");

		List<T> res = new ArrayList<>(this.size());

		for (int i = 0; i < size(); i++) {
			res.add(op.apply(i, this.value[i]));
		}

		return new DataList<>(DataType.OBJECT, res);
	}

	public LongList applyWhere(Mask mask, long newVal) {
		// explicit parameter types required to resolve ambiguity
		return this.applyWithIndex((int i, long val) -> mask.getAt(i) ? newVal : val);
	}

	public LongList applyWhere(Mask mask, LongBinaryOperator mapper) {
		return this.applyWithIndex((int i, long val) -> mask.getAt(i) ? mapper.applyAsLong(i, val) : val);
	}

	public LongList applyWhere(Mask mask, long trueVal, long falseVal) {
		return this.applyWithIndex((int i, long val) -> mask.getAt(i) ? trueVal : falseVal);
	}

	public LongList putAt(Mask mask, TwoInts values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(mask, values.a(), values.b());
	}

	public LongList putAt(Mask mask, long newVal) {
		return this.applyWhere(mask, newVal);
	}

	public LongList applyWhere(LongPredicate test, long newVal) {
		return this.apply(val -> test.test(val) ? newVal : null);
	}

	public LongList putAt(LongPredicate test, long newVal) {
		return this.applyWhere(test, newVal);
	}

	public LongList applyWhere(LongPredicate test, long trueValue, long falseValue) {
		return this.apply(val -> test.test(val) ? trueValue : falseValue);
	}

	public LongList putAt(LongPredicate test, TwoInts values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(test, values.a(), values.b());
	}

	public LongList applyWhere(LongPredicate test, LongUnaryOperator valueMapper) {
		return this.test(test).applyWhereTrue(i -> valueMapper.applyAsLong(this.getAt(i)),
				l -> LongList.of(l.stream().mapToLong(Long::longValue).toArray()));
	}

	public LongList getAt(LongPredicate predicate) {
		return this.getAt(this.test(predicate));
	}

	public LongList getAt(int from, int to) {
		return this.getAt(IntRange.of(from, to));
	}

	public LongList putAt(LongPredicate test, LongUnaryOperator valueMapper) {
		return this.applyWhere(test, valueMapper);
	}

	@Override
	public DataType dataType() {
		return DataType.LONG;
	}

	@Override
	public int size() {
		return this.value.length;
	}

	// TODO: implement index()
	@Override
	public Index index() {
		return null;
	}

	@Override
	public LongList asLong() {
		return this;
	}

	///////////// -- end completing methods --
}
