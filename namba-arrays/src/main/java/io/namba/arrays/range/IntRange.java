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
 * 
 * @author Ernest Kiwele
 */
public class IntRange {
	private int start = 0;
	private int end;
	private int step = 1;
	private int signum;

	private IntRange(int start, int end, int step) {
		if (0 == step) {
			throw new IllegalArgumentException("Step == 0");
		}

		this.start = start;
		this.end = end;
		this.step = step;
		this.signum = (int) Math.signum(step);
	}

	public static IntRange of(int start, int end, int step) {
		return new IntRange(start, end, (int) Math.signum((double) end - start) * Math.abs(step));
	}

	public static IntRange of(int end) {
		return of(0, end, (int) Math.signum(end));
	}

	public static IntRange of(int start, int end) {
		return of(start, end, (int) Math.signum((double) end - start));
	}

	public void forEach(IntConsumer c) {
		this.stream().forEach(Objects.requireNonNull(c));
	}

	public IntStream stream() {
		IntRangeIterator it = IntRangeIterator.of(this);
		return IntStream.iterate(it.next(), i -> it.hasNext(), i -> it.next());
	}

	public static void main(String[] args) {
		IntRange.of(40, 40, 3).forEach(System.out::println);
	}

	public static class IntRangeIterator {
		private boolean begun = false;
		private int i;
		private IntRange range;

		private IntRangeIterator(IntRange range) {
			this.i = Objects.requireNonNull(range).start;
			this.range = range;
		}

		public static IntRangeIterator of(IntRange range) {
			return new IntRangeIterator(range);
		}

		public boolean hasNext() {
			return (int) Math
					.signum((double) this.range.end - (this.begun ? this.i : this.range.start)) == this.range.signum;
		}

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
}
