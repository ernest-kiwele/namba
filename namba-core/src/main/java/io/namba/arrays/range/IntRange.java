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

import java.util.Objects;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * <p>
 * A simple representation of int ranges, with some methods to produce sequences
 * of numbers with optional custom steps.
 * </p>
 * <p>
 * A range is lazy. It computes its numbers on demand. In the same way, it can
 * be used to generate a corresponding <code>IntStream</code> for other
 * processing.
 * </p>
 * 
 * 
 * @author Ernest Kiwele
 */
public class IntRange {

	private final int start;
	private final int end;
	private final int step;
	private final int signum;

	private IntRange(int start, int end, int step) {
		if (0 == step) {
			throw new IllegalArgumentException("Step == 0");
		}

		this.start = start;
		this.end = end;

		// if end == start, then inferring signum may result in step == 0.
		if (end == start) {
			this.step = step;
		} else {
			this.step = (int) Math.signum((double) end - start) * Math.abs(step);
		}
		this.signum = (int) Math.signum(this.step);
	}

	/**
	 * Create a range between two ints with the given step separating consecutive
	 * numbers in the sequence.
	 * 
	 * Examples:
	 * 
	 * <pre>
	 *     IntRange.of(1, 10, 2) -> 1, 3, 5, 7, 9
	 *     IntRange.of(0, 6, 1)  -> 0, 1, 2, 3, 4, 5
	 * </pre>
	 * 
	 * <code>start</code> and <code>end</code> are not validated to ensure
	 * <code>end > start</code>. A negative <code>step</code> is inferred when
	 * appropriate, even if the step value passed is positive.
	 * 
	 * <pre>
	 *     IntRange.of(10, 1, 2)  -> 10, 8, 6, 4, 2
	 * </pre>
	 * 
	 * Again, even with a negative step, the <code>end</code> value is excluded.
	 * 
	 * @param start
	 *            The first number of the sequence.
	 * @param end
	 *            The last number of the sequence. It's excluded.
	 * @param step
	 *            The difference between consecutive numbers
	 * @return A new IntRange that can be used to provide values per the specified
	 *         sequence.
	 */
	public static IntRange of(int start, int end, int step) {
		return new IntRange(start, end, step);
	}

	/**
	 * Create an IntRange to generate a sequence between 0 and the given
	 * <code>end</code> value, with step 1.
	 * 
	 * The following two statements are equivalent:
	 * 
	 * <pre>
	 *     IntRange.of(10)
	 *     IntRange.of(0, 10, 1)
	 * </pre>
	 * 
	 * The same applies even if the given <code>end</code> is negative:
	 * 
	 * <pre>
	 *     IntRange.of(-5)  -> 0, -1, -2, -3, -4
	 * </pre>
	 *
	 * @param end
	 *            The end (exclusive) of the range.
	 * @return A new IntRange to generate numbers between 0 and the given number,
	 *         with step 1.
	 * @see {@link #of(int, int, int)}
	 */
	public static IntRange of(int end) {
		return of(0, end, (int) Math.signum(end));
	}

	/**
	 * Creates an IntRange between the given numbers, with step 1. The following two
	 * expressions are equivalent.
	 * 
	 * <pre>
	 *     IntRange.of(2, 6)
	 *     IntRange.of(2, 6, 1)
	 * </pre>
	 * 
	 * @param start
	 *            The inclusive start of the range.
	 * @param end
	 *            The exclusive end value of the range.
	 * @return A new IntRange.
	 * @see {@link #of(int, int, int)}
	 */
	public static IntRange of(int start, int end) {
		return of(start, end, (int) Math.signum((double) end - start));
	}

	/**
	 * Calls the given consumer for each element in this sequence.
	 * 
	 * @param c
	 *            The operation to be called. May not be null.
	 * @throws NullPointerException
	 *             If c is null.
	 */
	public void forEach(IntConsumer c) {
		Objects.requireNonNull(c, "operation c may not be null");
		this.stream().forEach(Objects.requireNonNull(c));
	}

	/**
	 * Creates an <code>IntStream</code> that will have each element of this range.
	 * 
	 * @return A stream with elements from this range.
	 */
	public IntStream stream() {
		IntRangeIterator it = IntRangeIterator.of(this);
		return IntStream.iterate(it.next(), i -> it.hasNext(), i -> it.next());
	}

	/**
	 * An iterator used to lazily generate integers in a defined sequence.
	 * 
	 * @author Ernest Kiwele
	 */
	public static class IntRangeIterator {
		private boolean begun = false;
		private int i;
		private IntRange range;

		private IntRangeIterator(IntRange range) {
			this.i = Objects.requireNonNull(range).start;
			this.range = range;
		}

		/**
		 * Create an IntRangeIterator for an IntRange
		 * 
		 * @param range
		 *            The range to create this iterator from.
		 * @return An new iterator.
		 * @throws NullPointerException
		 *             if range is null.
		 */
		public static IntRangeIterator of(IntRange range) {
			return new IntRangeIterator(range);
		}

		/**
		 * Returns true if the sequence has at least one more element.
		 * 
		 * @return True if at least one more element can be produced.
		 */
		public boolean hasNext() {
			return (int) Math
					.signum((double) this.range.end - (this.begun ? this.i : this.range.start)) == this.range.signum;
		}

		/**
		 * Returns the next element in the sequence. Callers must check with
		 * <code>hasNext()</code> before calling this method. The behavior is undefined
		 * otherwise.
		 * 
		 * @return The next element in the sequence.
		 */
		public int next() {
			int tmp;
			if (!this.begun) {
				tmp = this.range.start;
				this.begun = true;
			} else {
				tmp = this.i + this.range.step;
			}
			this.i = tmp;
			return tmp;
		}
	}

	@Override
	public String toString() {
		return this.start + ":" + this.end + ":" + this.step;
	}
}
