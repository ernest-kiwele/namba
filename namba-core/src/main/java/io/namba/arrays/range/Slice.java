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

package io.namba.arrays.range;

import java.util.stream.IntStream;

/**
 * <p>
 * A representation of a range targeted by a slice. This is an implementation
 * for index-based slices, i.e., an object intended to be used to select indices
 * on a scale.
 * </p>
 * <p>
 * A slice can be bounded on one or both ends. An unbounded slice is created
 * with Integer's extreme values as <code>from</code> and <code>to</code> , but
 * the processing, which is context-sensitive, interprets the "bounded" flags
 * and works with those rather.
 * </p>
 * <p>
 * The scale's right value is always exclusive.
 * </p>
 * 
 * @author Ernest Kiwele
 */
public class Slice {

	private static final Slice UNBOUNDED = new Slice(0, false, 0, false, 1);

	private final int from;
	private final boolean leftBounded;
	private final int to;
	private final boolean rightBounded;
	private final int step;

	/**
	 * Creates a scale with the given values.
	 * 
	 * @param from
	 *            The left bound of the slice. This value is inclusive.
	 * @param leftBounded
	 *            A flag indicating whether the left boundary applies. If true, the
	 *            slice is created with <code>Integer.MIN_VALUE</code> as left
	 *            bound, although processing is expected to be context-specific.
	 * @param to
	 *            The right bound of the slice. This value is exclusive.
	 * @param rightBounded
	 *            A flag indicating whether the right boundary applies. If true, the
	 *            slice is created with <code>Integer.MAX_VALUE</code> as right
	 *            boundary, although the actual interpretation will be
	 *            context-sensitive.
	 * @param step
	 *            The step used as interval of ones from left to right bounds. Step
	 *            will be stored as its absolute value. If the scale's
	 *            <code>from</code> and <code>to</code> values have a positive
	 *            difference, the execution will revert to a negative scale.
	 */
	public Slice(int from, boolean leftBounded, int to, boolean rightBounded, int step) {
		this.leftBounded = leftBounded;
		this.from = leftBounded ? from : Integer.MIN_VALUE;
		this.rightBounded = rightBounded;
		this.to = rightBounded ? to : Integer.MAX_VALUE;
		this.step = Math.abs(step);
	}

	/**
	 * Creates a bounded scale with the given range.
	 * 
	 * @param from
	 *            The left boundary of the scale.
	 * @param to
	 *            The right boundary of the scale.
	 * @param step
	 *            The interval of values skipped.
	 * @see {@link #SliceScale(int, boolean, int, boolean, int) SliceScale} for
	 *      instantiation details.
	 */
	public Slice(int from, int to, int step) {
		this(from, true, to, true, step);
	}

	/**
	 * Instantiate a new scale with the defined range data.
	 * 
	 * @param from
	 *            The left boundary of the scale.
	 * @param to
	 *            The right boundary of the scale.
	 * @param step
	 *            The interval of values skipped.
	 * @see {@link #SliceScale(int, boolean, int, boolean, int) SliceScale} for
	 *      instantiation details.
	 */
	public static Slice of(int from, int to, int step) {
		return new Slice(from, to, step);
	}

	/**
	 * Create a new scale with step 1, starting at zero and ending at the given
	 * number.
	 * 
	 * @param to
	 *            The right boundary of the scale.
	 * @return The newly created scale object.
	 */
	public static Slice of(int to) {
		return of(0, to, 1);
	}

	/**
	 * Create a scale with the given boundaries, using 1 as step.
	 * 
	 * @param from
	 *            The left bound of the scale
	 * @param to
	 *            The exclusive right bound of the scale
	 * @return The newly created scale
	 */
	public static Slice of(int from, int to) {
		return new Slice(from, to, 1);
	}

	/**
	 * Create an unbounded (on both ends) scale with 1 as step.
	 * 
	 * @return A new unbounded scale.
	 * @implNote This method will return a singleton instance, as objects are
	 *           immutable
	 */
	public static Slice unbounded() {
		return UNBOUNDED;
	}

	/**
	 * Create a left-bounded scale using the given value as the starting point, 1 as
	 * the step, and an unbounded right end.
	 * 
	 * @param from
	 *            The left bound of the scale.
	 * @return A new left-bounded scale.
	 */
	public static Slice from(int from) {
		return new Slice(from, true, Integer.MAX_VALUE, false, 1);
	}

	/**
	 * Create a right-bounded scale using the given value as the end point, 1 as the
	 * step, and an unbounded left end.
	 * 
	 * @param to
	 *            The right bound of the scale.
	 * @return A new right-bounded scale.
	 */
	public static Slice to(int to) {
		return new Slice(Integer.MIN_VALUE, false, to, true, 1);
	}

	/**
	 * Returns the left boundary of the scale. This will return Integer.MIN_VALUE if
	 * the scale is unbounded to the left.
	 * 
	 * @return The left bound.
	 */
	public int from() {
		return this.from;
	}

	/**
	 * Returns the right boundary of the scale. This will return Integer.MAX_VALUE
	 * if the scale is unbounded to the right.
	 * 
	 * @return The right bound.
	 */
	public int to() {
		return this.to;
	}

	/**
	 * Returns the step of the scale.
	 * 
	 * @return Scale step.
	 */
	public int step() {
		return this.step;
	}

	/**
	 * Returns a flag indicating whether the scale is bounded to the left.
	 * 
	 * @return true if the scale is bounded to the left, false otherwise.
	 */
	public boolean leftBounded() {
		return this.leftBounded;
	}

	/**
	 * Returns a flag indicating whether the scale is bounded to the right.
	 * 
	 * @return true if the scale is bounded to the right, false otherwise.
	 */
	public boolean rightBounded() {
		return this.rightBounded;
	}

	/**
	 * Returns the left boundary of the scale. This will return Integer.MIN_VALUE if
	 * the scale is unbounded to the left.
	 * 
	 * @return The left bound.
	 */
	public int getFrom() {
		return from;
	}

	/**
	 * Returns the right boundary of the scale. This will return Integer.MAX_VALUE
	 * if the scale is unbounded to the right.
	 * 
	 * @return The right bound.
	 */
	public int getTo() {
		return to;
	}

	/**
	 * Returns the step of the scale.
	 * 
	 * @return Scale step.
	 */
	public int getStep() {
		return step;
	}

	/**
	 * Returns a flag indicating whether the scale is bounded to the left.
	 * 
	 * @return true if the scale is bounded to the left, false otherwise.
	 */
	public boolean isLeftBounded() {
		return leftBounded;
	}

	/**
	 * Returns a flag indicating whether the scale is bounded to the right.
	 * 
	 * @return true if the scale is bounded to the right, false otherwise.
	 */
	public boolean isRightBounded() {
		return rightBounded;
	}

	public IntStream applyAsIndexStream(int length) {

		if (length < 0) {
			throw new IllegalArgumentException("length cannot be <= 0");
		}

		int minStart = -(Math.abs(length) - 1);
		int start = this.leftBounded ? Math.max(minStart, this.from) : 0;
		int end = this.rightBounded ? Math.min(this.to, length) : length;

		return IntRange.of(start, end, this.step).stream().map(i -> i < 0 ? i + length : i)
				.filter(i -> i >= 0 && i < length);
	}

	public int[] apply(int length) {
		return this.applyAsIndexStream(length).toArray();
	}

	public int[] call(int length) {
		return this.apply(length);
	}
}
