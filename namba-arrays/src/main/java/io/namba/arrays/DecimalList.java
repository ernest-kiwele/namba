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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import io.namba.Namba;
import io.namba.arrays.data.IndexedObject;
import io.namba.arrays.data.tuple.Two;
import io.namba.arrays.range.IntRange;
import io.namba.functions.DecimalRef;
import io.namba.functions.DecimalRef.DecimalPredicate;
import io.namba.functions.DecimalTest;
import io.namba.functions.NambaMath;

/**
 * 
 * @author Ernest Kiwele
 */
public class DecimalList extends DataList<BigDecimal> {

	public static final MathContext DEFAULT_MATH_CONTEXT = MathContext.DECIMAL64;
	public final MathContext mathContext;
	public static final BigDecimal MINUS_ONE = BigDecimal.ONE.negate();

	protected DecimalList(List<BigDecimal> is, Index index, MathContext mathContext) {
		super(DataType.BIGDECIMAL, is, index);
		this.mathContext = mathContext == null ? DEFAULT_MATH_CONTEXT : mathContext;
	}

	protected DecimalList(List<BigDecimal> is, Index index) {
		this(is, index, null);
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

	public DecimalList withMathContext(MathContext context) {
		return new DecimalList(this.value, this.index, context);
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
	public DecimalList getAt(IntStream stream) {
		return DecimalList.of(Objects.requireNonNull(stream).filter(i -> i >= 0 && i < this.value.size())
				.mapToObj(this.value::get).toArray(i -> new BigDecimal[i]));
	}

	public DecimalList getAt(Mask mask) {
		return getAt(mask.truthy().value);
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
		return new DecimalList(this.apply(i -> i.multiply(n, this.mathContext)));
	}

	public DecimalList multiply(DecimalList n) {
		return this.zip(n, (a, b) -> a.multiply(b, this.mathContext));
	}

	public DecimalList minus(BigDecimal n) {
		return this.apply(i -> i.subtract(n, this.mathContext));
	}

	public DecimalList minus(DecimalList n) {
		return this.zip(n, (a, b) -> a.subtract(b, this.mathContext));
	}

	public DecimalList plus(BigDecimal n) {
		return this.apply(i -> i.add(n, this.mathContext));
	}

	public DecimalList plus(DecimalList n) {
		return this.zip(n, (a, b) -> a.add(b, this.mathContext));
	}

	public DecimalList divide(BigDecimal n) {
		return this.apply(i -> i.divide(n, this.mathContext));
	}

	public DecimalList divide(DecimalList n) {
		return this.zip(n, (a, b) -> a.divide(b, this.mathContext));
	}

	public DecimalList power(int n) {
		return this.apply(i -> i.pow(n, this.mathContext));
	}

	public DecimalList power(IntList n) {
		List<BigDecimal> bd = new ArrayList<>();
		for (int i = 0; i < this.size(); i++) {
			bd.add(this.value.get(i).pow(n.getAt(i), this.mathContext));
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
		return this.apply(v -> v.multiply(v, this.mathContext));
	}

	public DecimalList squareRoot() {
		return this.apply(v -> v.sqrt(this.mathContext));
	}

	public DecimalList sqrt() {
		return this.squareRoot();
	}

	// TODO: logarithm calculations
	// TODO: log(e, x)
	// TODO: pow(2, x)

	// increment/decrement
	public DecimalList next() {
		return this.apply(v -> v.add(BigDecimal.ONE));
	}

	public DecimalList previous() {
		return this.apply(v -> v.subtract(BigDecimal.ONE));
	}

	// bitwise operators
	// TODO: Bitwise operations

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
			a[i] = this.value.get(i) != null && this.value.get(i).equals(other.value.get(i));
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

	public DecimalRef where(DecimalPredicate p) {
		return DecimalRef.where(p, this);
	}

	// Reduction
	public BigDecimal sum(boolean skipNans, BigDecimal nanValue) {
		if (this.value.isEmpty())
			return null;

		BigDecimal v = BigDecimal.ZERO;
		for (int i = 0; i < this.value.size(); i++) {
			BigDecimal e = this.value.get(i);

			if (null != e) {
				v = v.add(e, this.mathContext);
			} else if (skipNans) {
				continue;
			} else if (null == nanValue) {
				return null;
			} else {
				v = v.add(nanValue, this.mathContext);
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
				v = v.multiply(e, this.mathContext);
			} else if (skipNans) {
				continue;
			} else if (null == nanValue) {
				return null;
			} else {
				v = v.multiply(nanValue, this.mathContext);
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
		return sum.divide(BigDecimal.valueOf(this.size()), this.mathContext);
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

		return max == null || min == null ? null : max.subtract(min, this.mathContext);
	}

	public BigDecimal peakToPeak() {
		return this.ptp();
	}

	public DecimalList clip(BigDecimal low, BigDecimal high) {
		return new DecimalList(this.value.stream().filter(i -> low.compareTo(i) <= 0 && high.compareTo(i) >= 0)
				.collect(Collectors.toList()));
	}

	public BigDecimal populationVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean, this.mathContext).pow(2))
				.reduce(BigDecimal.ZERO, (a, b) -> a.add(b, this.mathContext))
				.divide(BigDecimal.valueOf(this.value.size()));
	}

	public BigDecimal sampleVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean).pow(2))
				.reduce(BigDecimal.ZERO, (a, b) -> a.add(b, this.mathContext))
				.divide(BigDecimal.valueOf(this.value.size() - 1l), this.mathContext);
	}

	// population std
	public BigDecimal std() {
		return this.populationVar().sqrt(this.mathContext);
	}

	public BigDecimal sampleStd() {
		return this.sampleVar().sqrt(this.mathContext);
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

	/**
	 * Counts non-null elements
	 * 
	 * @return
	 */
	public int count() {
		return (int) this.stream().filter(Objects::nonNull).count();
	}

	public DecimalList distinct() {
		return new DecimalList(this.stream().distinct().collect(Collectors.toList()));
	}

	public DecimalList unique() {
		return this.distinct();
	}

	public DecimalList dropDuplicates() {
		return this.dropDuplicates(false);
	}

	public DecimalList dropDuplicates(boolean keepLast) {
		List<BigDecimal> data = (keepLast ? this.reverseStream() : this.stream()).distinct()
				.collect(Collectors.toList());
		if (keepLast) {
			List<BigDecimal> ordered = new ArrayList<>();

			for (BigDecimal bd : data) {
				ordered.add(0, bd);
			}

			data = ordered;
		}
		return new DecimalList(data);
	}

	// TODO: look into storing a "sorted" flag with corresponding order.
	public DecimalList nLargest(int n) {
		List<BigDecimal> copy = new ArrayList<>(this.value);
		copy.sort(Comparator.reverseOrder());
		return DecimalList.of(copy.subList(0, n), null);
	}

	public DecimalList nSmallest(int n) {
		List<BigDecimal> copy = new ArrayList<>(this.value);
		copy.sort(Comparator.naturalOrder());
		return DecimalList.of(copy.subList(0, n), null);
	}

	public int nUnique() {
		return (int) this.value.stream().distinct().count();
	}

	public BigDecimal[] toArray() {
		return this.value.toArray(i -> new BigDecimal[i]);
	}

	public BigDecimal agg(BinaryOperator<BigDecimal> reducer) {
		return this.value.stream().reduce(reducer).orElse(null);
	}

	public BigDecimal aggregate(BinaryOperator<BigDecimal> reducer) {
		return this.agg(reducer);
	}

	public BigDecimal agg(BigDecimal identity, BinaryOperator<BigDecimal> reducer) {
		return this.value.stream().reduce(identity, reducer);
	}

	public BigDecimal aggregate(BigDecimal identity, BinaryOperator<BigDecimal> reducer) {
		return this.agg(identity, reducer);
	}

	public Mask test(DecimalTest test) {
		return test.test(this);
	}

	public boolean all(DecimalTest test) {
		return test.all(this);
	}

	public boolean any(DecimalTest test) {
		return test.any(this);
	}

	// Concatenate two or more Series.
	public DecimalList concat(DecimalList other) {
		List<BigDecimal> all = new ArrayList<>(this.value);
		all.addAll(other.value);

		return DecimalList.of(all, null);
	}

	public DecimalList append(DecimalList other) {
		return this.concat(other);
	}

	/*
	 * Return the integer indices that would sort the Series values.
	 * 
	 * Override ndarray.argsort. Argsorts the value, omitting NA/null values, and
	 * places the result in the same locations as the non-NA values.
	 */
	public IntList argSort() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)))
				.sorted(Comparator.comparing(IndexedObject::value)).mapToInt(IndexedObject::index).toArray());
	}

	public IntList argSortReversed() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)))
				.sorted(Comparator.comparing(IndexedObject<BigDecimal>::value).reversed())
				.mapToInt(IndexedObject::index).toArray());
	}

	/*
	 * Compute the lag-N autocorrelation.
	 * 
	 * This method computes the Pearson correlation between the Series and its
	 * shifted self.
	 */
	public BigDecimal autoCorrelation(int shifts) {
		return this.correlation(this.shift(shifts));
	}

	public BigDecimal autoCorr(int shifts) {
		return this.autoCorrelation(shifts);
	}

	/*
	 * Synonym for DataFrame.fillna() with method='bfill'.
	 */
	public DecimalList backFill() {

		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal lastValid = null;
		for (int i = v.length - 1; i >= 0; i--) {
			BigDecimal val = this.value.get(i);

			if (null != val) {
				lastValid = this.value.get(i);
			} else if (lastValid != null) {
				val = lastValid;
			}

			v[i] = val;
		}

		return of(v);
	}

	public DecimalList forwardFill() {

		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal lastValid = null;
		for (int i = 0; i < v.length; i++) {
			BigDecimal val = this.value.get(i);

			if (null != val) {
				lastValid = this.value.get(i);
			} else if (lastValid != null) {
				val = lastValid;
			}

			v[i] = val;
		}

		return of(v);
	}

	/*
	 * Fill NA/NaN values using the specified method.
	 * 
	 * method : {‘backfill’, ‘bfill’, ‘pad’, ‘ffill’, None}, default None
	 */
	public DecimalList fillNa(BigDecimal bd) {

		Objects.requireNonNull(bd, "fill value may not be null");

		BigDecimal[] v = new BigDecimal[this.size()];

		for (int i = 0; i < v.length; i++) {
			BigDecimal val = this.value.get(i);
			if (null == val) {
				v[i] = bd;
			} else {
				v[i] = val;
			}
		}

		return of(v);
	}

	// public DecimalList filter(Object... rowLabels) {
	//
	// }

	/*
	 * Return boolean Series equivalent to left <= series <= right.
	 * 
	 * This function returns a boolean vector containing True wherever the
	 * corresponding Series element is between the boundary values left and right.
	 * NA values are treated as False.
	 */
	public Mask between(BigDecimal low, BigDecimal high) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal bd = this.value.get(i);
			b[i] = low.compareTo(bd) <= 0 && high.compareTo(bd) >= 0;
		}

		return Mask.of(b);
	}

	/*
	 * Trim values at input threshold(s).
	 * 
	 * Assigns values outside boundary to boundary values. Thresholds can be
	 * singular values or array like, and in the latter case the clipping is
	 * performed element-wise in the specified axis.
	 */
	public DecimalList clipToBoundaries(BigDecimal low, BigDecimal high) {
		BigDecimal[] b = new BigDecimal[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal bd = this.value.get(i);
			b[i] = NambaMath.max(low, NambaMath.min(high, bd));
		}

		return of(b);
	}

	/*
	 * Combine the Series with a Series or scalar according to func.
	 * 
	 * Combine the Series and other using func to perform elementwise selection for
	 * combined Series. fill_value is assumed when value is missing at some index
	 * from one of the two objects being combined.
	 */
	public DecimalList combine(DecimalList other, BinaryOperator<BigDecimal> combiner) {
		return this.zip(other, combiner);
	}

	/*
	 * Combine Series values, choosing the calling Series’s values first.
	 * 
	 * (kinda like nvl)
	 */
	public DecimalList combineFirst(DecimalList other) {
		return this.zip(other, NambaMath::firstNonNull);
	}

	/*
	 * Compare to another Series and show the differences.
	 * 
	 */
	public Table compare(DecimalList other) {
		Mask nonEqual = this.ne(other);

		return Table.of(Arrays.asList(this.getAt(nonEqual), other.getAt(nonEqual)), null);
	}

	public BigDecimal correlation(DecimalList other) {

		DecimalList thisMeanDiff = this.minus(this.mean());
		DecimalList otherMeanDiff = other.minus(other.mean());

		BigDecimal a = thisMeanDiff.multiply(otherMeanDiff).sum();
		BigDecimal b = thisMeanDiff.square().sum().multiply(otherMeanDiff.square().sum()).sqrt(DEFAULT_MATH_CONTEXT);

		return a.divide(b, DEFAULT_MATH_CONTEXT);
	}

	public BigDecimal corr(DecimalList other) {
		return this.correlation(other);
	}

	// public BigDecimal covariance(DecimalList other) {
	//
	// }
	//
	// public BigDecimal cov(DecimalList other) {
	// return this.covariance(other);
	// }

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
				last = last.add(v, this.mathContext);
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
				last = last.multiply(v, this.mathContext);
				r[i] = last;
			}
		}

		return DecimalList.of(r);
	}

	public DecimalList cumMax() {
		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal val = null;
		for (int i = 0; i < v.length; i++) {
			val = NambaMath.max(value.get(i), val);
			v[i] = val;
		}

		return of(v);
	}

	public DecimalList cumMin() {
		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal val = null;
		for (int i = 0; i < v.length; i++) {
			val = NambaMath.min(value.get(i), val);
			v[i] = val;
		}

		return of(v);
	}

	/*
	 * count 3.0 mean 2.0 std 1.0 min 1.0 25% 1.5 50% 2.0 75% 2.5 max 3.0 dtype:
	 * float64
	 */
	// TODO: Implement
	public DecimalList describe() {
		return null;
	}

	/*
	 * First discrete difference of element.
	 * 
	 * Calculates the difference of a Series element compared with another element
	 * in the Series (default is element in previous row).
	 */
	// TODO: implement
	public DecimalList diff() {
		return null;
	}

	/*
	 * Return a new Series with missing values removed.
	 * 
	 * See the User Guide for more on which values are considered missing, and how
	 * to work with missing data.
	 */
	public DecimalList dropNa() {
		return this.getAt(this.isNa().negate());
	}

	// TODO: Overload this
	// public Grouping groupBy(Function<BigDecimal, Object> classifier) {
	//
	// }

	public DecimalList head() {
		return getAt(IntRange.of(SUMMARY_SIZE));
	}

	public DecimalList head(int n) {
		return getAt(IntRange.of(n));
	}

	public DecimalList tail() {
		return getAt(IntRange.of(this.size() - SUMMARY_SIZE, this.size()));
	}

	public DecimalList tail(int n) {
		return getAt(IntRange.of(this.size() - n, this.size()));
	}

	public Table hist() {
		Map<BigDecimal, Long> groups = this.value.stream()
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		BigDecimal[] keys = new BigDecimal[groups.size()];
		int[] counts = new int[groups.size()];

		List<Entry<BigDecimal, Long>> lst = groups.entrySet().stream().collect(Collectors.toList());

		for (int i = 0; i < lst.size(); i++) {
			Entry<BigDecimal, Long> entry = lst.get(i);
			keys[i] = entry.getKey();
			counts[i] = entry.getValue().intValue(); // should never overflow
		}

		return Table.of(null, DecimalList.of(keys), IntList.of(counts));
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

	public boolean isUnique() {
		return this.distinct().count() == this.size();
	}

	public Mask isNa() {
		boolean[] v = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			v[i] = this.getAt(i) == null;
		}

		return Mask.of(v);
	}

	/*
	 * Return the first element of the underlying data as a python scalar.
	 */
	public BigDecimal item() {
		return this.value.isEmpty() ? null : this.value.get(0);
	}

	/*
	 * Lazily iterate over (index, value) tuples.
	 * 
	 * This method returns an iterable tuple (index, value). This is convenient if
	 * you want to create a lazy iterator.
	 */
	// TODO: implement index-based iteration
	// public Iterator<Two<Object, BigDecimal>> items() {
	//
	// }
	//
	// public Stream<Two<Object, BigDecimal>> itemStream() {
	//
	// }

	public Stream<IndexedObject<BigDecimal>> indexItemStream() {
		return IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)));
	}

	public Stream<IndexedObject<BigDecimal>> indexItemStreamReversed() {
		int total = this.size();
		return IntStream.range(0, this.size()).map(i -> total - i - 1)
				.mapToObj(i -> IndexedObject.of(i, this.value.get(i)));
	}

	/*
	 * Return unbiased kurtosis over requested axis.
	 * 
	 * Kurtosis obtained using Fisher’s definition of kurtosis (kurtosis o
	 */
	// TODO: Implement kurtosis
	// public BigDecimal kurtosis() {
	//
	// }
	//
	// public BigDecimal kurt() {
	//
	// }

	/*
	 * Return index for last non-NA/null value.
	 */
	public int lastValidIndex() {
		return this.indexItemStreamReversed().filter(v -> null != v.value()).findFirst().map(IndexedObject::getIndex)
				.orElse(-1);
	}

	/*
	 * Return the mean absolute deviation of the values for the requested axis.
	 */
	// TODO: Implement this
	// public DecimalList meanAbsoluteDeviation() {
	//
	// }
	// public DecimalList mad() {
	// return this.meanAbsoluteDeviation();
	// }

	/**
	 * replaces with the given value where the predicate evaluates to true
	 * 
	 * @param cond
	 * @param val
	 * @return
	 */
	public DecimalList replaceWhere(Predicate<BigDecimal> cond, BigDecimal val) {
		Objects.requireNonNull(cond);
		Objects.requireNonNull(val);

		BigDecimal[] v = new BigDecimal[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal value = this.getAt(i);
			if (cond.test(value)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return DecimalList.of(v);
	}

	public DecimalList replaceWhere(Mask cond, BigDecimal val) {
		Objects.requireNonNull(cond);
		Objects.requireNonNull(val);

		BigDecimal[] v = new BigDecimal[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal value = this.getAt(i);
			if (cond.getAt(i)) {
				v[i] = val;
			} else {
				v[i] = value;
			}
		}

		return DecimalList.of(v);
	}

	/*
	 * Return the median of the values for the requested axis.
	 * 
	 * Note: this skips na values
	 */
	public BigDecimal median() {

		DecimalList sorted = this.sorted();
		DecimalList withoutNa = sorted.getAt(sorted.isNa());

		if (withoutNa.size() == 0)
			return null;

		if (withoutNa.size() % 2 == 1) {
			return withoutNa.getAt(withoutNa.size() / 2 + 1);
		} else {
			int medLocation = withoutNa.size() / 2;
			return NambaMath.mean(withoutNa.getAt(medLocation), withoutNa.getAt(medLocation + 1));
		}
	}

	/*
	 * Return value at the given quantile.
	 */
	public BigDecimal quantile(BigDecimal quantile) {
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
	}

	/*
	 * Compute numerical data ranks (1 through n) along axis.
	 * 
	 * By default, equal values are assigned a rank that is the average of the ranks
	 * of those values.
	 */
	public IntList rank() {
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
	}

	/*
	 * Needs to be defined with window objects.
	 */
	public Map<Two<BigDecimal, BigDecimal>, DecimalList> rolling() {

	}

	public DecimalList round(int decimals) {

	}

	public DecimalList round(IntList decimals) {

	}

	public DecimalList sample(double fraction) {
		return this.sample((int) (this.size() * fraction));
	}

	public DecimalList sample(int size) {

	}

	public DecimalList sample(int size, int randomState) {
	}

	/*
	 * Return unbiased standard error of the mean over requested axis.
	 * 
	 * Normalized by N-1 by default. This can be changed using the ddof argument
	 */
	public BigDecimal meanStandardError() {

	}

	/*
	 * Shift index by desired number of periods with an optional time freq.
	 * 
	 * 
	 */
	public DecimalList shift(int n) {
		BigDecimal[] list = new BigDecimal[this.size()];

		for (int i = n; i < list.length; i++)
			list[i] = this.value.get(i - n);

		return DecimalList.of(list);
	}

	/*
	 * Return unbiased skew over requested axis.
	 * 
	 * Normalized by N-1.
	 */
	public BigDecimal skew() {

	}

	/*
	 * Sort Series by index labels.
	 * 
	 * Returns a new Series sorted by label if inplace argument is False, otherwise
	 * updates the original series and returns None.
	 */
	public DecimalList sortIndex() {

	}

	public DecimalList sorted(boolean descending, boolean naFirst) {

	}

	public DecimalList sorted() {
		return this.sorted(false, false);
	}

	public DecimalList where(Mask mask) {

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
