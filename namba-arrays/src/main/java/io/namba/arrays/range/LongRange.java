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
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class LongRange {

	private long start = 0;
	private long end;
	private long step = 1;
	private long signum;

	private LongRange(long start, long end, long step) {
		if (0 == step) {
			throw new IllegalArgumentException("Step == 0");
		}

		this.start = start;
		this.end = end;
		this.step = step;
		this.signum = (long) Math.signum(step);
	}

	public static LongRange of(long start, long end, long step) {
		return new LongRange(start, end, (long) Math.signum((double) end - start) * step);
	}

	public static LongRange of(long end) {
		return of(0, end, (long) Math.signum(end));
	}

	public static LongRange of(long start, long end) {
		return of(start, end, (long) Math.signum((double) end - start));
	}

	public void forEach(LongConsumer c) {
		Objects.requireNonNull(c, "consumer may not be null");
		this.stream().forEach(c);
	}

	public LongStream stream() {
		LongRangeIterator it = LongRangeIterator.of(this);
		return LongStream.iterate(it.next(), i -> it.hasNext(), i -> it.next());
	}

	public static void main(String[] args) {
		LongRange.of(140, 40, 3).forEach(System.out::println);
	}

	public static class LongRangeIterator {
		private boolean begun = false;
		private long i;
		private LongRange range;

		private LongRangeIterator(LongRange range) {
			this.i = Objects.requireNonNull(range).start;
			this.range = range;
		}

		public static LongRangeIterator of(LongRange range) {
			return new LongRangeIterator(range);
		}

		public boolean hasNext() {
			return (long) Math
					.signum((double) this.range.end - (this.begun ? this.i : this.range.start)) == this.range.signum;
		}

		public long next() {
			long tmp;
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
