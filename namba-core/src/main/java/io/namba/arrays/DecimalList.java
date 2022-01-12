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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
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
import io.namba.arrays.agg.DecimalGrouping;
import io.namba.arrays.data.IndexedObject;
import io.namba.arrays.data.IntData;
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
	private static final DecimalFormat DEFAULT_FORMAT = new DecimalFormat("#,###.######");

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

	/**
	 * Returns a <code>DecimalList</code> with absolute values of each element in
	 * this list.
	 * 
	 * @return A new decimal list with absolute values of this list's elements.
	 */
	public DecimalList abs() {
		return this.apply(BigDecimal::abs);
	}

	/**
	 * An alias of {@link #abs()}
	 */
	public DecimalList absolute() {
		return this.abs();
	}

	/**
	 * Creates a decimal list with this list's values negated.
	 * 
	 * @return A new list with this list's values negated.
	 */
	public DecimalList negative() {
		return this.apply(BigDecimal::negate);
	}

	/**
	 * Returns this list's values as is.
	 * 
	 * @return This list's values with their current signs.
	 * @implNote The current implementation returns the current instance.
	 */
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

	/**
	 * Returns a decimal list with elements corresponding to the sign of elements.
	 * 
	 * @return A new list with <code>signum</code> values from this list's elements.
	 * @implNote The "signum" values are as per <code>BigDecimal</code>'s
	 *           <code>sinum</code> implementation.
	 */
	public DecimalList signum() {
		return this.apply(i -> null == i ? null : BigDecimal.valueOf(i.signum()));
	}

	/**
	 * An alias for {@link #signum()}
	 */
	public DecimalList sign() {
		return this.signum();
	}

	/**
	 * Finds the mode in this list's data set.
	 */
	public BigDecimal mode() {
		return this.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting())).entrySet()
				.stream().max(Map.Entry.comparingByValue()).orElseThrow().getKey();
	}

	/**
	 * Treat this list's elements as double (using
	 * {@link BigDecimal#doubleValue()}), apply the given <code>op</code> operation
	 * to compute new values used to create a new decimal list's elements.
	 * 
	 * <p>
	 * All <code>null</code> values will be returned without being using
	 * <code>op</code>.
	 * </p>
	 * 
	 * @param op
	 *            The mapping used to compute new values for each entry from the
	 *            current list.
	 * @return A new decimal list with decimal values created from the result of
	 *         <code>op</code>'s mapping.
	 * @implNote The operation does not perform any range check for doubles passed
	 *           to the mapping operation, so overflows and underflows will be
	 *           ignored.
	 */
	public DecimalList applyDoubleOperation(DoubleUnaryOperator op) {
		return new DecimalList(
				this.value.stream().map(v -> null == v ? null : BigDecimal.valueOf(op.applyAsDouble(v.doubleValue())))
						.collect(Collectors.toList()));
	}

	/**
	 * Returns the trigonometric sine of each element of this list as input angle.
	 * 
	 * @return A new decimal list with <code>sine</code> results corresponding to
	 *         this list's elements.
	 * @implNote This operation converts elements to <code>double</code> values
	 *           using {@link BigDecimal#doubleValue()}, so overflows and underflows
	 *           are ignored.
	 * @see {@link java.lang.Math#sin}
	 */
	public DecimalList sin() {
		return this.applyDoubleOperation(Math::sin);
	}

	/**
	 * Returns the trigonometric cosine of each element of this list as input angle.
	 * 
	 * @return A new decimal list with <code>cosine</code> results corresponding to
	 *         this list's elements.
	 * @implNote This operation converts elements to <code>double</code> values
	 *           using {@link BigDecimal#doubleValue()}, so overflows and underflows
	 *           are ignored.
	 * @see {@link java.lang.Math#cos}
	 */
	public DecimalList cos() {
		return this.applyDoubleOperation(Math::cos);
	}

	/**
	 * Returns the trigonometric tangent of each element of this list as input
	 * angle.
	 * 
	 * @return A new decimal list with <code>tangent</code> results corresponding to
	 *         this list's elements.
	 * @implNote This operation converts elements to <code>double</code> values
	 *           using {@link BigDecimal#doubleValue()}, so overflows and underflows
	 *           are ignored.
	 * @see {@link java.lang.Math#tan}
	 */
	public DecimalList tan() {
		return this.applyDoubleOperation(Math::tan);
	}

	/**
	 * An alias for {@link #asin()}
	 */
	public DecimalList arcsin() {
		return this.asin();
	}

	/**
	 * Returns the trigonometric arc sine of each element of this list as input
	 * angle.
	 * 
	 * @return A new decimal list with arc sine results corresponding to this list's
	 *         elements.
	 * @implNote This operation converts elements to <code>double</code> values
	 *           using {@link BigDecimal#doubleValue()}, so overflows and underflows
	 *           are ignored.
	 * @see {@link java.lang.Math#asin}
	 */
	public DecimalList asin() {
		return this.applyDoubleOperation(Math::asin);
	}

	/**
	 * An alias for {@link #acos()}.
	 */
	public DecimalList arccos() {
		return this.acos();
	}

	/**
	 * Returns the trigonometric arc cosine of each element of this list as input
	 * angle.
	 * 
	 * @return A new decimal list with arc cosine results corresponding to this
	 *         list's elements.
	 * @implNote This operation converts elements to <code>double</code> values
	 *           using {@link BigDecimal#doubleValue()}, so overflows and underflows
	 *           are ignored.
	 * @see {@link java.lang.Math#acos}
	 */
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

	/**
	 * Computes the square root of each element and returns a new decimal list.
	 * 
	 * @return A new list with elements corresponding to square roots of this
	 *         instance's elements.
	 */
	public DecimalList squareRoot() {
		return this.apply(v -> v.sqrt(this.mathContext));
	}

	/**
	 * An alias for {@link #squareRoot()}.
	 */
	public DecimalList sqrt() {
		return this.squareRoot();
	}

	// TODO: logarithm calculations
	// TODO: log(e, x)
	// TODO: pow(2, x)

	/**
	 * Increments each of this list's elements by 1.
	 * 
	 * @return A new list with the result of that addition.
	 */
	public DecimalList next() {
		return this.apply(v -> v.add(BigDecimal.ONE));
	}

	/**
	 * Decrements each of this list's elements by 1.
	 * 
	 * @return A new list with the result of that subtraction.
	 */
	public DecimalList previous() {
		return this.apply(v -> v.subtract(BigDecimal.ONE));
	}

	// bitwise operators
	// TODO: Bitwise operations

	/**
	 * {@link An alias for #equals(BigDecimal)}
	 */
	public Mask eq(BigDecimal other) {
		return this.equals(other);
	}

	/**
	 * Returns a mask with the result of equality test between each element and the
	 * given decimal value.
	 * 
	 * @param other
	 *            A value to test equality with against all elements.
	 * @return A mask with results corresponding to all elements of this list.
	 */
	public Mask equals(BigDecimal other) {
		return this.test(other::equals);
	}

	/**
	 * An alias for {@link DecimalList#equals(DecimalList)}
	 */
	public Mask eq(DecimalList other) {
		return this.equals(other);
	}

	/**
	 * Returns a mask with the result of one-to-one equality tests between each
	 * element and the given decimal list's values.
	 * 
	 * @param other
	 *            A list supplying elements to test equality against.
	 * @return A mask with results corresponding to all elements of both list.
	 * @throws IllegalArgumentException
	 *             If the sizes of the two lists do not match.
	 */
	public Mask equals(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).equals(other.value.get(i));
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is less than each corresponding element
	 * of the given list.
	 * 
	 * @param other
	 *            A list of elements to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask lt(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) < 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is less than the given element,
	 * returning results corresponding to this list's elements.
	 * 
	 * @param other
	 *            The element to compare this list's elements to.
	 * @return A mask with results of that comparison.
	 */
	public Mask lt(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) < 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is less than or equal to each
	 * corresponding element of the given list.
	 * 
	 * @param other
	 *            A list of elements to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask le(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) <= 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is less than or equal to the given
	 * value.
	 * 
	 * @param other
	 *            A value to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask le(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) < 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is greater than each corresponding
	 * element of the given list.
	 * 
	 * @param other
	 *            A list of elements to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask gt(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) > 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is greater the given value.
	 * 
	 * @param other
	 *            A value to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask gt(BigDecimal other) {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other) > 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is greater than or equal to each
	 * corresponding element of the given list.
	 * 
	 * @param other
	 *            A list of elements to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
	public Mask ge(DecimalList other) {
		this.verifySizeMatch(this, other);

		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < a.length; i++) {
			a[i] = this.value.get(i) != null && this.value.get(i).compareTo(other.value.get(i)) >= 0;
		}

		return Mask.of(a);
	}

	/**
	 * Tests that each element of this list is greater or equal to the given value.
	 * 
	 * @param other
	 *            A value to compare elements of this list to.
	 * @return A mask with results of that comparison.
	 */
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

	/**
	 * Compute the difference between the max and the min values in this list.
	 * 
	 * @return The difference between the max and the min values. If the collection
	 *         is empty, null is returned.
	 */
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

	/**
	 * An alias of {@link #ptp()}
	 */
	public BigDecimal peakToPeak() {
		return this.ptp();
	}

	/**
	 * Create a new decimal list with values from this list filtered to fit the
	 * given range, both <code>low</code> and <code>high</code> inclusive.
	 * 
	 * @param low
	 *            The minimum value to clip to.
	 * @param high
	 *            The maximum value to clip to.
	 * @return A new decimal list containing values from this list that fit in the
	 *         given range.
	 */
	public DecimalList clip(BigDecimal low, BigDecimal high) {
		return new DecimalList(this.value.stream().filter(i -> low.compareTo(i) <= 0 && high.compareTo(i) >= 0)
				.collect(Collectors.toList()));
	}

	/**
	 * Computes the population variance of values in this decimal list.
	 */
	public BigDecimal populationVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean, this.mathContext).pow(2))
				.reduce(BigDecimal.ZERO, (a, b) -> a.add(b, this.mathContext))
				.divide(BigDecimal.valueOf(this.value.size()));
	}

	/**
	 * Compute the sample variance of values in this decimal list.
	 */
	public BigDecimal sampleVar() {
		BigDecimal mean = this.mean();
		return this.value.stream().map(i -> i.subtract(mean).pow(2))
				.reduce(BigDecimal.ZERO, (a, b) -> a.add(b, this.mathContext))
				.divide(BigDecimal.valueOf(this.value.size() - 1l), this.mathContext);
	}

	/**
	 * Compute the population standard deviation from observations in this decimal
	 * list.
	 * 
	 * @see {@link #populationVar()}
	 */
	public BigDecimal std() {
		return this.populationVar().sqrt(this.mathContext);
	}

	/**
	 * Compute the sample standard deviation from observations in this decimal list.
	 * 
	 * @see {@link #sampleVar()}
	 */
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
	 * Returns indices of non-zero elements. Nulls are not excluded from the
	 * returned list.
	 */
	public DecimalList nonZero() {
		return new DecimalList(
				this.value.stream().filter(i -> null == i || !i.equals(BigDecimal.ZERO)).collect(Collectors.toList()));
	}

	/**
	 * Returns true if none of the elements is equal to 0.
	 * 
	 * @implNote The comparison is performed using BigDecimal.ZERO.equals(x) for x
	 *           in this list. Nulls are not considered "zero"
	 */
	public boolean noneZero() {
		for (BigDecimal i : this.value) {
			if (BigDecimal.ZERO.equals(i)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Returns true if any of the elements is not equal to 0.
	 * 
	 * @implNote The comparison is performed using BigDecimal.ZERO.equals(x) for x
	 *           in this list. Nulls are not considered "zero"
	 */
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
	 */
	public int count() {
		return (int) this.stream().filter(Objects::nonNull).count();
	}

	/**
	 * Compute a decimal list containing unique values from this list.
	 * 
	 * @return A new decimal list with distinct values from this list.
	 */
	public DecimalList distinct() {
		return new DecimalList(this.stream().distinct().collect(Collectors.toList()));
	}

	/**
	 * An alias for {@link #distinct()}
	 */
	public DecimalList unique() {
		return this.distinct();
	}

	public DecimalList dropDuplicates() {
		return this.dropDuplicates(false);
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
	/**
	 * Return the given number of this list's largest values. This is equivalent to
	 * slicing a reverse-sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> largest values.
	 */
	public DecimalList nLargest(int n) {
		List<BigDecimal> copy = new ArrayList<>(this.value);
		copy.sort(Comparator.reverseOrder());
		return DecimalList.of(copy.subList(0, n), null);
	}

	/**
	 * Return the given number of this list's smallest values. This is equivalent to
	 * slicing a sorted version of this list using the given number.
	 * 
	 * @param n
	 *            The number of elements to return.
	 * @return A new list with the <code>n</code> smallest values.
	 */
	public DecimalList nSmallest(int n) {
		List<BigDecimal> copy = new ArrayList<>(this.value);
		copy.sort(Comparator.naturalOrder());
		return DecimalList.of(copy.subList(0, n), null);
	}

	/**
	 * Returns the number of unique elements in this list.
	 */
	public int nUnique() {
		return (int) this.value.stream().distinct().count();
	}

	/**
	 * Creates an array containing this list's elements.
	 */
	public BigDecimal[] toArray() {
		return this.value.toArray(i -> new BigDecimal[i]);
	}

	/**
	 * Perform a reduction using the given binary operation.
	 * 
	 * @param reducer
	 *            The binary operation to perform the aggregation with. This may not
	 *            be null.
	 * @return The aggregated value, or null if the collection is empty.
	 */
	public BigDecimal agg(BinaryOperator<BigDecimal> reducer) {
		return this.value.stream().reduce(Objects.requireNonNull(reducer, "reducer may not be null")).orElse(null);
	}

	/**
	 * An alias for {@link #agg(BinaryOperator)}
	 */
	public BigDecimal aggregate(BinaryOperator<BigDecimal> reducer) {
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
	public BigDecimal agg(BigDecimal identity, BinaryOperator<BigDecimal> reducer) {
		return this.value.stream().reduce(identity, reducer);
	}

	/**
	 * An alias for {@link #agg(BigDecimal, BinaryOperator)}
	 */
	public BigDecimal aggregate(BigDecimal identity, BinaryOperator<BigDecimal> reducer) {
		return this.agg(identity, reducer);
	}

	public Mask test(DecimalTest test) {
		return test.test(this);
	}

	public Mask test(DecimalPredicate test) {
		return test.test(this);
	}

	public boolean all(DecimalTest test) {
		return test.all(this);
	}

	public boolean any(DecimalTest test) {
		return test.any(this);
	}

	// Concatenate two or more Series.
	/**
	 * Concatenate this list and <code>other</code>
	 * 
	 * @param other
	 *            List to append to this
	 * @return A new decimal list with this and <code>other</code> joined.
	 */
	public DecimalList concat(DecimalList other) {
		List<BigDecimal> all = new ArrayList<>(this.value);
		all.addAll(other.value);

		return DecimalList.of(all, null);
	}

	/**
	 * An alias for {@link #concat(DataList)}
	 */
	public DecimalList append(DecimalList other) {
		return this.concat(other);
	}

	/**
	 * Return the integer indices that would sort the list's values.
	 */
	public IntList argSort() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)))
				.sorted(Comparator.comparing(IndexedObject::value)).mapToInt(IndexedObject::index).toArray());
	}

	/**
	 * Return the integer indices that would reverse-sort the list's values.
	 */
	public IntList argSortReversed() {
		return IntList.of(IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)))
				.sorted(Comparator.comparing(IndexedObject<BigDecimal>::value).reversed())
				.mapToInt(IndexedObject::index).toArray());
	}

	/**
	 * Compute the lag-N autocorrelation.
	 * 
	 * This method computes the Pearson correlation between the list and its shifted
	 * self.
	 */
	public BigDecimal autoCorrelation(int shifts) {
		return this.correlation(this.shift(shifts));
	}

	/**
	 * An alias for {@link #autoCorrelation(int)}
	 */
	public BigDecimal autoCorr(int shifts) {
		return this.autoCorrelation(shifts);
	}

	/**
	 * Fills nulls with the next valid value found in the list.
	 */
	public DecimalList forwardFill() {

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

	/**
	 * Fills nulls with the previous valid value found in the list.
	 */
	public DecimalList backFill() {

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

	/**
	 * Fills nulls with the given value.
	 * 
	 * @param bd
	 *            The value to replace nulls with.
	 * @return A new decimal list with nulls replaced.
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

	/**
	 * Returns a mask equivalent to a test for low <= x <= high for x in this list.
	 */
	public Mask between(BigDecimal low, BigDecimal high) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal bd = this.value.get(i);
			b[i] = low.compareTo(bd) <= 0 && high.compareTo(bd) >= 0;
		}

		return Mask.of(b);
	}

	/**
	 * Trim values at input thresholds. Assigns values outside boundary to boundary
	 * values.
	 * 
	 * The difference btween this method and {@link #clip(BigDecimal, BigDecimal)}
	 * is that this replaces values out of bounds with the closest boundary element,
	 * rather than excluding them.
	 * 
	 * @see {@link #clip(BigDecimal, BigDecimal)} for a similar method.
	 */
	public DecimalList clipToBoundaries(BigDecimal low, BigDecimal high) {
		BigDecimal[] b = new BigDecimal[this.size()];

		for (int i = 0; i < this.size(); i++) {
			BigDecimal bd = this.value.get(i);
			b[i] = NambaMath.max(low, NambaMath.min(high, bd));
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
	public DecimalList combine(DecimalList other, BinaryOperator<BigDecimal> combiner) {
		return this.zip(Objects.requireNonNull(other), Objects.requireNonNull(combiner));
	}

	/**
	 * Combine list values, choosing this list’s values first, only replacing them
	 * with the given series if null.
	 * 
	 * @param other
	 *            The other list supplying alternative values.
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

	/**
	 * Compute the correlation between this and the <code>other</code> correlation.
	 * 
	 * @param other
	 *            The other list.
	 */
	public BigDecimal correlation(DecimalList other) {

		DecimalList thisMeanDiff = this.minus(this.mean());
		DecimalList otherMeanDiff = other.minus(other.mean());

		BigDecimal a = thisMeanDiff.multiply(otherMeanDiff).sum();
		BigDecimal b = thisMeanDiff.square().sum().multiply(otherMeanDiff.square().sum()).sqrt(DEFAULT_MATH_CONTEXT);

		return a.divide(b, DEFAULT_MATH_CONTEXT);
	}

	/**
	 * An alias for {@link #correlation(DecimalList)}
	 */
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

	/**
	 * Compute cumulative sum of values in this list.
	 * 
	 * @return A new decimal list with cumulative sums.
	 */
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

	/**
	 * Compute cumulative product of values in this list.
	 * 
	 * @return A new decimal list with cumulative products.
	 */
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

	/**
	 * Compute cumulative max of values in this list.
	 * 
	 * @return A new decimal list with cumulative maxima.
	 */
	public DecimalList cumMax() {
		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal val = null;
		for (int i = 0; i < v.length; i++) {
			val = NambaMath.max(value.get(i), val);
			v[i] = val;
		}

		return of(v);
	}

	/**
	 * Compute cumulative min of values in this list.
	 * 
	 * @return A new decimal list with cumulative minima.
	 */
	public DecimalList cumMin() {
		BigDecimal[] v = new BigDecimal[this.size()];

		BigDecimal val = null;
		for (int i = 0; i < v.length; i++) {
			val = NambaMath.min(value.get(i), val);
			v[i] = val;
		}

		return of(v);
	}

	/**
	 * Apply a row-wise cumulative computation of values using the given function.
	 * For the first value, the function is called with null.
	 * 
	 * @param aggregator
	 *            The function computing accumulated values.
	 * @return A new DecimalList object with the result of that accumulation.
	 */
	public DecimalList cumFunc(BinaryOperator<BigDecimal> aggregator) {
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
	 * @return A new DecimalList object with the result of that accumulation.
	 */
	public DecimalList cumFunc(BinaryOperator<BigDecimal> aggregator, boolean skipFirst) {

		if (0 == this.size())
			return DecimalList.of(new BigDecimal[0]);

		List<BigDecimal> d = new ArrayList<>(this.size());
		BigDecimal prev;
		int offset;

		if (skipFirst) {
			d.add(null);
			prev = this.getAt(0);
			offset = 1;
		} else {
			prev = null;
			offset = 0;
		}

		for (int i = offset; i < this.size(); i++) {
			d.add(prev = aggregator.apply(prev, this.value.get(i)));
		}

		return new DecimalList(d);
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

	/**
	 * Return a new decimal list with missing values removed.
	 */
	public DecimalList dropNa() {
		return this.getAt(this.isNa().negate());
	}

	// TODO: Overload this
	// public Grouping groupBy(Function<BigDecimal, Object> classifier) {
	//
	// }

	/**
	 * Return the first few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public DecimalList head() {
		return getAt(IntRange.of(SUMMARY_SIZE));
	}

	/**
	 * Return the first <code>n</code> values of this list.
	 */
	public DecimalList head(int n) {
		return getAt(IntRange.of(n));
	}

	/**
	 * Return the last few values of this list. The default value is specified by
	 * {@link NambaList#SUMMARY_SIZE}
	 */
	public DecimalList tail() {
		return getAt(IntRange.of(this.size() - SUMMARY_SIZE, this.size()));
	}

	/**
	 * Return the last <code>n</code> values of this list.
	 */
	public DecimalList tail(int n) {
		return getAt(IntRange.of(this.size() - n, this.size()));
	}

	/**
	 * Returns a histogram of values in this decimal list.
	 * 
	 * @return A <code>Table</code> with values and their counts.
	 */
	public Table hist() {
		Map<BigDecimal, Integer> groups = this.histogram();

		BigDecimal[] keys = new BigDecimal[groups.size()];
		int[] counts = new int[groups.size()];

		List<Two<BigDecimal, Long>> lst = this.valueCounts();

		for (int i = 0; i < lst.size(); i++) {
			Two<BigDecimal, Long> entry = lst.get(i);
			keys[i] = entry.a();
			counts[i] = entry.b().intValue(); // should never overflow
		}

		return Table.of(null, DecimalList.of(keys), IntList.of(counts));
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
		List<Two<BigDecimal, Double>> lst = this.normalizedValueCounts(percentage);

		BigDecimal[] keys = new BigDecimal[lst.size()];
		double[] counts = new double[lst.size()];

		for (int i = 0; i < lst.size(); i++) {
			Two<BigDecimal, Double> entry = lst.get(i);
			keys[i] = entry.a();
			counts[i] = entry.b().intValue(); // should never overflow
		}

		return Table.of(null, DecimalList.of(keys), DoubleList.of(counts));
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
		return this.distinct().count() == this.size();
	}

	/**
	 * Returns a mask indicating whether the value is null for each position.
	 * 
	 * @return A new mask with booleans indicating nulls.
	 */
	public Mask isNa() {
		boolean[] v = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			v[i] = this.getAt(i) == null;
		}

		return Mask.of(v);
	}

	/**
	 * Return the first element of the underlying data.
	 * 
	 * @return Null if the list is empty, the first element otherwise.
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

	/**
	 * Returns a {@link java.util.stream.Stream stream} of objects holding each
	 * element of this list and its corresponding index.
	 * 
	 * @return A stream that supplies indexed elements from this decimal list.
	 * @see {@link #indexItemStreamReversed()}
	 */
	public Stream<IndexedObject<BigDecimal>> indexItemStream() {
		return IntStream.range(0, this.size()).mapToObj(i -> IndexedObject.of(i, this.value.get(i)));
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

	/**
	 * Returns index for last non-NA/null value.
	 * 
	 * @return The last index of a non-null value. If the list is empty or contains
	 *         no null value, -1 is returned.
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
	 * Replaces with the given value where the predicate evaluates to true.
	 * 
	 * @param cond
	 *            The condition to test with.
	 * @param val
	 *            The value to replace matching elements.
	 * @return A new list with matching elements replaced.
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

	/**
	 * Replaces with the given value where the mask is set to true.
	 * 
	 * @param cond
	 *            The mask with indexes to replace set to true.
	 * @param val
	 *            The value to replace matching elements.
	 * @return A new list with matching elements replaced.
	 */
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

	/**
	 * Return the median of the values. Note that this skips null values.
	 */
	public BigDecimal median() {

		DecimalList sorted = this.sorted();
		DecimalList withoutNa = sorted.getAt(sorted.isNa().negate());

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
	// TODO: implement quantile
	// public BigDecimal quantile(BigDecimal quantile) {
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
	// public Map<Two<BigDecimal, BigDecimal>, DecimalList> rolling() {
	//
	// }

	/**
	 * Round values to the given number of decimals. The number can be negative,
	 * which simply reduces the precision of values.
	 * 
	 * @param decimals
	 *            Number of decimals to round to.
	 * @return A new decimal list with values rounded to the given number of
	 *         decimals.
	 * @see {@link #roundTo(IntList)}
	 */
	public DecimalList roundTo(int decimals) {
		BigDecimal[] v = new BigDecimal[this.size()];

		for (int i = 0; i < v.length; i++) {
			BigDecimal val = this.value.get(i);
			v[i] = null == val ? null : NambaMath.truncate(val, decimals);
		}

		return DecimalList.of(v);
	}

	/**
	 * Round values to the number of decimals given in the given int list. The
	 * number can be negative, which simply reduces the precision of values.
	 * 
	 * @param decimals
	 *            An int index with numbers of decimals to round to.
	 * @return A new decimal list with values rounded to the given numbers of
	 *         decimals.
	 * @see {@link #roundTo(int)}
	 */
	public DecimalList roundTo(IntList decimals) {
		BigDecimal[] v = new BigDecimal[this.size()];

		for (int i = 0; i < v.length; i++) {
			BigDecimal val = this.value.get(i);
			v[i] = null == val ? null : NambaMath.truncate(val, decimals.getAt(i));
		}

		return DecimalList.of(v);
	}

	/**
	 * Extract a sample of values from this decimal list.
	 * 
	 * @param fraction
	 *            The fraction of the size of this list to sample. Must be a valid
	 *            ratio: 0.0 < sample < 1.0
	 * @return A new list with a sample from this list.
	 */
	public DecimalList sample(double fraction) {
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
	public DecimalList sample(int size) {
		return this.getAt(IntData.instance().randomArray(size, 0, this.size()));
	}

	public DecimalList sample(int size, long randomState) {
		return this.getAt(IntData.instance().randomArray(randomState, size, 0, this.size()));
	}

	/*
	 * Return unbiased standard error of the mean over requested axis.
	 * 
	 * Normalized by N-1 by default. This can be changed using the ddof argument
	 */
	// TODO: Implement
	// public BigDecimal meanStandardError() {
	//
	// }

	/*
	 * Shift index by desired number of periods with an optional time freq.
	 * 
	 * 
	 */
	@Override
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
	// TODO: Implement skew()
	// public BigDecimal skew() {
	//
	// }

	/*
	 * Sort Series by index labels.
	 * 
	 * Returns a new Series sorted by label if inplace argument is False, otherwise
	 * updates the original series and returns None.
	 */
	// TODO: Implement
	// public DecimalList sortIndex() {
	//
	// }

	public DecimalList sorted(boolean descending, boolean naFirst) {
		List<BigDecimal> v = new ArrayList<>(this.value);

		Comparator<BigDecimal> comparator = descending ? Comparator.reverseOrder() : Comparator.naturalOrder();

		if (naFirst) {
			comparator = Comparator.nullsFirst(comparator);
		} else {
			comparator = Comparator.nullsLast(comparator);
		}

		v.sort(comparator);

		return new DecimalList(v);
	}

	public DecimalList sorted() {
		return this.sorted(false, false);
	}

	public DecimalList where(Mask mask) {
		return this.getAt(mask);
	}

	@Override
	public <K> DecimalGrouping<K> groupBy(Function<BigDecimal, K> classifier) {
		return DecimalGrouping.of(this,
				IntStream.range(0, size()).mapToObj(i -> Two.of(i, classifier.apply(this.value.get(i))))
						.collect(Collectors.groupingBy(Two::b, Collectors.mapping(Two::a, Collectors.toList()))));
	}

	@Override
	public StringList string() {
		return StringList.of(
				this.value.stream().map(o -> o == null ? null : DEFAULT_FORMAT.format(o)).collect(Collectors.toList()));
	}

	public StringList string(DecimalFormat numberFormat) {
		return StringList.of(
				this.value.stream().map(o -> o == null ? null : numberFormat.format(o)).collect(Collectors.toList()));
	}

	// Cookbook

	public Two<DecimalList, DecimalList> partition(DecimalPredicate predicate) {
		return Objects.requireNonNull(predicate, "predicate may not be null").test(this).partition(this,
				DecimalList::new);
	}

	public Two<DecimalList, DecimalList> partition(Mask mask) {
		return Objects.requireNonNull(mask, "mask may not be null").partition(this, DecimalList::new);
	}

	public DecimalList applyWhere(Mask mask, BigDecimal newVal) {
		return new DecimalList(this.applyWithIndex((i, val) -> mask.getAt(i) ? newVal : val));
	}

	public DecimalList applyWhere(Mask mask, BiFunction<Integer, BigDecimal, BigDecimal> mapper) {
		return new DecimalList(this.applyWithIndex((i, val) -> mask.getAt(i) ? mapper.apply(i, val) : val));
	}

	public DecimalList applyWhere(Mask mask, BigDecimal trueVal, BigDecimal falseVal) {
		return new DecimalList(this.applyWithIndex((i, val) -> mask.getAt(i) ? trueVal : falseVal));
	}

	public DecimalList putAt(Mask mask, Two<BigDecimal, BigDecimal> values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(mask, values.a(), values.b());
	}

	public DecimalList putAt(Mask mask, BigDecimal newVal) {
		return this.applyWhere(mask, newVal);
	}

	public DecimalList applyWhere(Predicate<BigDecimal> test, BigDecimal newVal) {
		return new DecimalList(this.apply(val -> test.test(val) ? newVal : null));
	}

	public DecimalList putAt(Predicate<BigDecimal> test, BigDecimal newVal) {
		return this.applyWhere(test, newVal);
	}

	public DecimalList applyWhere(Predicate<BigDecimal> test, BigDecimal trueValue, BigDecimal falseValue) {
		return new DecimalList(this.apply(val -> test.test(val) ? trueValue : falseValue));
	}

	public DecimalList putAt(Predicate<BigDecimal> test, Two<BigDecimal, BigDecimal> values) {
		Objects.requireNonNull(values, "replacement values may not be null");
		return this.applyWhere(test, values.a(), values.b());
	}

	public DecimalList applyWhere(Predicate<BigDecimal> test, UnaryOperator<BigDecimal> valueMapper) {
		return new DecimalList(this.test(test).applyWhereTrue(i -> valueMapper.apply(this.getAt(i))));
	}

	public DecimalList getAt(Predicate<BigDecimal> predicate) {
		return this.getAt(this.test(predicate));
	}

	public DecimalList getAt(int from, int to) {
		return this.getAt(IntRange.of(from, to));
	}

	public DecimalList putAt(Predicate<BigDecimal> test, UnaryOperator<BigDecimal> valueMapper) {
		return this.applyWhere(test, valueMapper);
	}

	/*
	 * public CategoryList cut(int bins) {
	 * 
	 * }
	 * 
	 * // bins by the number of labels, and labels into category public CategoryList
	 * cut(String... labels) {
	 * 
	 * }
	 * 
	 * public CategoryList cut(Function<BigDecimal, String> categoryMapper) {
	 * 
	 * }
	 * 
	 * public CategoryList cut(BigDecimal... bins) {
	 * 
	 * }
	 * 
	 * public CategoryList cut(List<Two<BigDecimal, String>> bins) {
	 * 
	 * }
	 * 
	 * public CategoryList cut(List<BigDecimal> bins, List<String> labels) {
	 * 
	 * }
	 * 
	 * // merging public <K> DataList<BigDecimal> join(DecimalList other,
	 * Function<BigDecimal, K> joinKeyMapper, JoinType how, JoinConstraint
	 * uniquenessConstraint) {
	 * 
	 * }
	 */

	public class LocationAccessor {

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
		Random random = new Random();
		var dl = Namba.instance().data.decimals.generate(10, () -> new BigDecimal(random.nextInt(100)));
		System.out.println(dl.groupBy(n -> n.compareTo(BigDecimal.valueOf(0.5)) > 0).min());
	}
}
