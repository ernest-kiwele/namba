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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

import io.namba.Namba;
import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 */
public class DecimalList extends DataList<BigDecimal> {

	public static final MathContext DEFAULT_MATH_CONTEXT = MathContext.DECIMAL64;
	public static final BigDecimal MINUS_ONE = BigDecimal.ONE.negate();

	protected DecimalList(List<BigDecimal> is, Index index) {
		super(DataType.BIGDECIMAL, is, index);
	}

	protected DecimalList(List<BigDecimal> is) {
		this(is, null);
	}

	protected DecimalList(DataList<BigDecimal> is, Index index) {
		this(is.value, index);
	}

	protected DecimalList(DataList<BigDecimal> is) {
		this(is.value, null);
	}

	public static DecimalList of(List<BigDecimal> v, Function<BigDecimal, Object> indexer) {
		return new DecimalList(v, Index.objectIndex(v, indexer));
	}

	public static DecimalList of(BigDecimal[] v) {
		return new DecimalList(Arrays.asList(v), null);
	}

	/// methods

	// indexing
	public DecimalList indexBy(Function<BigDecimal, Object> indexer) {
		return DecimalList.of(this.value, Objects.requireNonNull(indexer, "indexer is null"));
	}

	public DecimalList getByIndex(Object key) {
		if (null == this.index) {
			throw new IllegalStateException("list is not indexed");
		}

		return this.getAt(this.index.getByKey(key));
	}

	@Override
	public Index getIndex() {
		return index;
	}

	// utilities
	// public IntMatrix toMatrix(int width) {
	// return new IntMatrix(this.value, width);
	// }

	@Override
	public String toString() {
		return this.string().toString();
	}

	// accessors
	private DecimalList getAt(IntStream stream) {
		return DecimalList.of(Objects.requireNonNull(stream).filter(i -> i >= 0 && i < this.value.size())
				.mapToObj(this.value::get).toArray(i -> new BigDecimal[i]));
	}

	@Override
	public DecimalList getAt(int[] is) {
		return getAt(Arrays.stream(is));
	}

	public DecimalList take(int size) {
		return getAt(IntRange.of(size));
	}

	@Override
	public DecimalList getAt(IntRange range) {
		return getAt(range.stream());
	}

	@Override
	public DecimalList getAt(List<Integer> range) {
		return getAt(range.stream().mapToInt(Integer::intValue));
	}

	// operations

	public DecimalList zip(DecimalList other, BinaryOperator<BigDecimal> op) {
		if (this.size() != other.size()) {
			throw new IllegalStateException("Arrays are of different sizes");
		}

		List<BigDecimal> list = new ArrayList<>();
		Iterator<BigDecimal> it1 = this.iterator(), it2 = other.iterator();
		while (it1.hasNext()) {
			list.add(op.apply(it1.next(), it2.next()));
		}

		return new DecimalList(list, null);
	}

	public static DecimalList zip(DecimalList a, DecimalList b, BinaryOperator<BigDecimal> op) {
		return a.zip(b, op);
	}

	@Override
	public DecimalList apply(UnaryOperator<BigDecimal> op) {
		return new DecimalList(this.map(v -> null == v ? null : op.apply(v)));
	}

	public DecimalList multiply(BigDecimal n) {
		return new DecimalList(this.apply(i -> i.multiply(n)));
	}

	public DecimalList multiply(DecimalList n) {
		return this.zip(n, BigDecimal::multiply);
	}

	public DecimalList minus(BigDecimal n) {
		return this.apply(i -> i.subtract(n));
	}

	public DecimalList minus(DecimalList n) {
		return this.zip(n, BigDecimal::subtract);
	}

	public DecimalList plus(BigDecimal n) {
		return this.apply(i -> i.add(n));
	}

	public DecimalList plus(DecimalList n) {
		return this.zip(n, BigDecimal::add);
	}

	public DecimalList divide(BigDecimal n) {
		return this.apply(i -> i.divide(n));
	}

	public DecimalList divide(DecimalList n) {
		return this.zip(n, BigDecimal::divide);
	}

	public DecimalList power(int n) {
		return this.apply(i -> i.pow(n));
	}

	public DecimalList power(IntList n) {
		List<BigDecimal> bd = new ArrayList<>();
		for (int i = 0; i < this.size(); i++) {
			bd.add(this.value.get(i).pow(n.getAt(i)));
		}
		return new DecimalList(bd, null);
	}

	public DecimalList abs() {
		return this.apply(BigDecimal::abs);
	}

	public DecimalList absolute() {
		return this.abs();
	}

	public DecimalList negative() {
		return this.apply(BigDecimal::negate);
	}

	public DecimalList positive() {
		return this;
	}

	// public DecimalList mod(int other) {
	// int[] r = new int[this.value.length];
	//
	// for (int i = 0; i < r.length; i++)
	// r[i] = this.value[i] % other;
	//
	// return this.apply(i -> i.) of(r);
	// }

	// public DecimalList mod(DecimalList n) {
	// return this.zip(n, (a, b) -> a % b);
	// }

	public DecimalList signum() {
		return this.apply(i -> null == i ? null : BigDecimal.valueOf(i.signum()));
	}

	public DecimalList sign() {
		return this.signum();
	}

	public BigDecimal mode() {
		return this.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet()
				.stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
	}

	public DecimalList applyDoubleOperation(DoubleUnaryOperator op) {
		return new DecimalList(
				this.value.stream().map(v -> null == v ? null : BigDecimal.valueOf(op.applyAsDouble(v.doubleValue())))
						.collect(Collectors.toList()));
	}

	public DecimalList sin() {
		return this.applyDoubleOperation(Math::sin);
	}

	public DecimalList cos() {
		return this.applyDoubleOperation(Math::cos);
	}

	public DecimalList tan() {
		return this.applyDoubleOperation(Math::tan);
	}

	public DecimalList arcsin() {
		return this.asin();
	}

	public DecimalList asin() {
		return this.applyDoubleOperation(Math::asin);
	}

	public DecimalList arccos() {
		return this.acos();
	}

	public DecimalList acos() {
		return this.applyDoubleOperation(Math::acos);
	}

	public DecimalList arctan() {
		return this.acos();
	}

	public DecimalList atan() {
		return this.applyDoubleOperation(Math::atan);
	}

	public DecimalList hsin() {
		return this.applyDoubleOperation(Math::sinh);
	}

	public DecimalList sinh() {
		return this.hsin();
	}

	public DecimalList hcos() {
		return this.applyDoubleOperation(Math::cosh);
	}

	public DecimalList cosh() {
		return this.hcos();
	}

	public DecimalList htan() {
		return this.applyDoubleOperation(Math::tanh);
	}

	public DecimalList tanh() {
		return this.htan();
	}

	public DecimalList rad() {
		return this.applyDoubleOperation(Math::toRadians);
	}

	public DecimalList toRadians() {
		return this.rad();
	}

	public DecimalList square() {
		return this.apply(v -> v.multiply(v));
	}

	public DecimalList squareRoot() {
		return this.apply(v -> v.sqrt(DEFAULT_MATH_CONTEXT));
	}

	public DecimalList sqrt() {
		return this.squareRoot();
	}

	// TODO: logarithm calculations
	// public DecimalList log10() {
	// double[] a = new double[this.value.length];
	//
	// for (int i = 0; i < a.length; i++)
	// a[i] = Math.log10(this.value[i]);
	//
	// return this.apply(v -> ) DecimalList.of(a);
	// }

	// TODO: log(e, x)
	// public DecimalList log() {
	// double[] a = new double[this.value.length];
	//
	// for (int i = 0; i < a.length; i++)
	// a[i] = Math.log(this.value[i]);
	//
	// return DecimalList.of(a);
	// }

	// TODO: pow(2, x)
	// public DecimalList exp2() {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++)
	// a[i] = (int) Math.pow(2, this.value[i]);
	//
	// return this.apply(v -> v.pow) of(a);
	// }

	// increment/decrement
	public DecimalList next() {
		return this.apply(v -> v.add(BigDecimal.ONE));
	}

	public DecimalList previous() {
		return this.apply(v -> v.subtract(BigDecimal.ONE));
	}

	// bitwise operators
	// TODO: Bigwise operations
	// public DecimalList and(int other) {
	// return this.bitwiseAnd(other);
	// }
	//
	// public DecimalList and(DecimalList other) {
	// return this.bitwiseAnd(other);
	// }

	// public DecimalList bitwiseAnd(int other) {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] & other;
	// }
	//
	// return DecimalList.of(a);
	// }

	// public DecimalList bitwiseAnd(DecimalList other) {
	// this.verifySizeMatch(this, other);
	//
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] & other.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }

	// public DecimalList or(int other) {
	// return this.bitwiseOr(other);
	// }
	//
	// public DecimalList or(DecimalList other) {
	// return this.bitwiseOr(other);
	// }
	//
	// public DecimalList bitwiseOr(int other) {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] | other;
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList bitwiseOr(DecimalList other) {
	// this.verifySizeMatch(this, other);
	//
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] | other.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList xor(int other) {
	// return this.bitwiseXor(other);
	// }
	//
	// public DecimalList xor(DecimalList other) {
	// return this.bitwiseXor(other);
	// }
	//
	// public DecimalList bitwiseXor(int other) {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] ^ other;
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList bitwiseXor(DecimalList other) {
	// this.verifySizeMatch(this, other);
	//
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] ^ other.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList leftShift(int other) {
	// return this.bitwiseLeftShift(other);
	// }
	//
	// public DecimalList leftShift(DecimalList other) {
	// return this.bitwiseLeftShift(other);
	// }
	//
	// public DecimalList bitwiseLeftShift(int other) {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] << other;
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList bitwiseLeftShift(DecimalList other) {
	// this.verifySizeMatch(this, other);
	//
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] << other.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList rightShift(int other) {
	// return this.bitwiseRightShift(other);
	// }
	//
	// public DecimalList rightShift(DecimalList other) {
	// return this.bitwiseRightShift(other);
	// }
	//
	// public DecimalList bitwiseRightShift(int other) {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] >> other;
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList bitwiseRightShift(DecimalList other) {
	// this.verifySizeMatch(this, other);
	//
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = this.value[i] >> other.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }
	//
	// public DecimalList bitwiseNegate() {
	// int[] a = new int[this.value.length];
	//
	// for (int i = 0; i < a.length; i++) {
	// a[i] = ~this.value[i];
	// }
	//
	// return DecimalList.of(a);
	// }

	// Comparison
	public Mask eq(BigDecimal other) {
		return this.equals(other);
	}

	public Mask equals(BigDecimal other) {
		return this.test(other::equals);
	}

	public Mask eq(DecimalList other) {
		return this.equals(other);
	}

	public Mask equals(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) == null ? false : this.value.get(i).equals(other.value.get(i));
		}

		return Mask.of(a);
	}

	public Mask lt(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) < 0;
		}

		return Mask.of(a);
	}

	public Mask lt(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) < 0;
		}

		return Mask.of(a);
	}

	public Mask le(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) <= 0;
		}

		return Mask.of(a);
	}

	public Mask le(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) < 0;
		}

		return Mask.of(a);
	}

	public Mask gt(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) > 0;
		}

		return Mask.of(a);
	}

	public Mask gt(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) > 0;
		}

		return Mask.of(a);
	}

	public Mask ge(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) >= 0;
		}

		return Mask.of(a);
	}

	public Mask ge(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) >= 0;
		}

		return Mask.of(a);
	}

	public Mask ne(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && !this.value.get(i).equals(other.value.get(i));
		}

		return Mask.of(a);
	}

	public Mask ne(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && !this.value.get(i).equals(other);
		}

		return Mask.of(a);
	}

	// Reduction
	public BigDecimal sum(boolean skipNans, BigDecimal nanValue) {
		if (this.value.isEmpty())
			return null;

		BigDecimal v = BigDecimal.ZERO;
		for (int i = 0; i < this.value.size(); i++) {
			BigDecimal e = this.value.get(i);

			if (null != e) {
				v = v.add(e);
			} else if (skipNans) {
				continue;
			} else if (null == nanValue) {
				return null;
			} else {
				v = v.add(nanValue);
			}
		}

		return v;
	}

	public BigDecimal sum(boolean skipNans) {
		return this.sum(skipNans, null);
	}

	public BigDecimal sum(BigDecimal nanValue) {
		return this.sum(false, nanValue);
	}

	public BigDecimal sum() {
		return this.sum(false, BigDecimal.ZERO);
	}

	public BigDecimal product(boolean skipNans, BigDecimal nanValue) {
		if (this.value.isEmpty())
			return null;

		BigDecimal v = BigDecimal.ONE;
		for (int i = 0; i < this.value.size(); i++) {
			BigDecimal e = this.value.get(i);

			if (null != e) {
				v = v.add(e);
			} else if (skipNans) {
				continue;
			} else if (null == nanValue) {
				return null;
			} else {
				v = v.add(nanValue);
			}
		}

		return v;
	}

	public BigDecimal product(boolean skipNans) {
		return this.product(skipNans, null);
	}

	public BigDecimal product(BigDecimal nanValue) {
		return this.product(false, nanValue);
	}

	public BigDecimal product() {
		return this.product(false, BigDecimal.ONE);
	}

	public BigDecimal prod(boolean skipNans, BigDecimal nanValue) {
		return this.product(skipNans, nanValue);
	}

	public BigDecimal prod(boolean skipNans) {
		return this.product(skipNans);
	}

	public BigDecimal prod(BigDecimal nanValue) {
		return this.product(nanValue);
	}

	public BigDecimal prod() {
		return this.product();
	}

	// comparison filters out nulls
	public BigDecimal max() {
		return this.value.stream().filter(Objects::nonNull).max(BigDecimal::compareTo).orElse(null);
	}

	// comparison filters out nulls
	public BigDecimal min() {
		return this.value.stream().filter(Objects::nonNull).min(BigDecimal::compareTo).orElse(null);
	}

	public BigDecimal mean() {
		BigDecimal sum = this.sum();
		if (null == sum) {
			return null;
		}
		return sum.divide(BigDecimal.valueOf(this.size()), DEFAULT_MATH_CONTEXT);
	}

	public BigDecimal getSum() {
		return this.sum();
	}

	public BigDecimal getProduct() {
		return this.product();
	}

	public BigDecimal getMax() {
		return this.max();
	}

	public BigDecimal getMin() {
		return this.min();
	}

	public BigDecimal getMean() {
		return this.mean();
	}

	public int argmax() {
		return IntStream.range(0, this.value.size()).mapToObj(i -> Pair.of(i, this.value.get((i))))
				.max(Entry.comparingByValue()).map(Pair::getLeft).orElse(-1);
	}

	public int argmin() {
		return IntStream.range(0, this.value.size()).mapToObj(i -> Pair.of(i, this.value.get((i))))
				.min(Entry.comparingByValue()).map(Pair::getLeft).orElse(-1);
	}

	public BigDecimal ptp() {

		BigDecimal max = null;
		BigDecimal min = null;

		for (BigDecimal bd : this.value) {
			if (bd == null) {
				continue;
			}

			if (null == max)
				max = bd;
			else
				max = bd.compareTo(max) > 0 ? bd : max;

			if (null == min)
				min = bd;
			else
				min = bd.compareTo(min) < 0 ? bd : min;
		}

		return max == null || min == null ? null : max.subtract(min);
	}

	public BigDecimal peakToPeak() {
		return this.ptp();
	}

	public DecimalList clip(BigDecimal low, BigDecimal high) {
		return new DecimalList(this.value.stream().filter(i -> low.compareTo(i) <= 0 && high.compareTo(i) >= 0)
				.collect(Collectors.toList()));
	}

	public DecimalList cumSum() {
		if (this.value.isEmpty()) {
			return new DecimalList(Collections.emptyList());
		}

		BigDecimal[] r = new BigDecimal[this.value.size()];
		BigDecimal last = BigDecimal.ZERO;

		for (int i = 0; i < this.value.size(); i++) {
			BigDecimal v = this.value.get(i);
			if (null == v) {
				r[i] = null;
			} else {
				last = last.add(v);
				r[i] = last;
			}
		}

		return DecimalList.of(r);
	}

	public DecimalList cumProd() {
		if (this.value.isEmpty()) {
			return new DecimalList(Collections.emptyList());
		}

		BigDecimal[] r = new BigDecimal[this.value.size()];
		BigDecimal last = BigDecimal.ONE;

		for (int i = 0; i < this.value.size(); i++) {
			BigDecimal v = this.value.get(i);
			if (null == v) {
				r[i] = null;
			} else {
				last = last.multiply(v);
				r[i] = last;
			}
		}

		return DecimalList.of(r);
	}

	public BigDecimal populationVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean).pow(2)).reduce(BigDecimal.ZERO, BigDecimal::add)
				.divide(BigDecimal.valueOf(this.value.size()));
	}

	public BigDecimal sampleVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean).pow(2)).reduce(BigDecimal.ZERO, BigDecimal::add)
				.divide(BigDecimal.valueOf(this.value.size() - 1));
	}

	private void verifySizeMatch(DecimalList left, DecimalList right) {
		if (left.value.size() != right.value.size()) {
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

	public IntList asInt() {
		return ListCast.toInt(this);
	}

	// implementation
	@Override
	public DecimalList repeat(int n) {
		List<BigDecimal> v = new ArrayList<>();

		for (int i = 0; i < n; i++) {
			v.addAll(this.value);
		}

		return new DecimalList(v);
	}

	/**
	 * Returns indices of non-zero elements
	 * 
	 * @return
	 */
	public DecimalList nonZero() {
		return new DecimalList(
				this.value.stream().filter(i -> null == i || !i.equals(BigDecimal.ZERO)).collect(Collectors.toList()));
	}

	public boolean noneZero() {
		for (BigDecimal i : this.value) {
			if (BigDecimal.ZERO.equals(i)) {
				return false;
			}
		}

		return true;
	}

	public boolean anyNonZero() {
		for (BigDecimal i : this.value) {
			if (!BigDecimal.ZERO.equals(i)) {
				return true;
			}
		}

		return false;
	}

	@Override
	public StringList string() {
		return StringList.of(this.value.stream().map(Object::toString).collect(Collectors.toList()));
	}

	public class IndexAccessor {
		public DecimalList getAt(Object key) {
			return getByIndex(key);
		}

		public int getSize() {
			return index.getSize();
		}

		public Set<Object> getKeys() {
			return index.getKeys();
		}
	}

	public static void main(String[] args) {
		Namba nb = Namba.instance();

		DecimalList rr = nb.data.decimals.random(1, 20, 20.0, 300.0);
		System.out.println(rr);
		System.out.println(rr.argmin());
		System.out.println(rr.sum());
		System.out.println(rr.mean());
		System.out.println(rr.mean().multiply(BigDecimal.valueOf(20)));
	}
}