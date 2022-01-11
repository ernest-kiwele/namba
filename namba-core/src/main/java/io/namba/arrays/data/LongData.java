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

package io.namba.arrays.data;

import java.util.Arrays;
import java.util.Random;
import java.util.function.LongSupplier;
import java.util.stream.LongStream;

import io.namba.arrays.LongList;
import io.namba.arrays.range.LongRange;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class LongData {

	private static final LongData instance = new LongData();
	private final Random random = new Random();

	private LongData() {
	}

	public static LongData instance() {
		return instance;
	}

	public static LongData seed(long seed) {
		LongData d = new LongData();
		d.random.setSeed(seed);
		return d;
	}

	public long[] arrayOf(int size, long value) {
		long[] a = new long[size];
		Arrays.fill(a, value);
		return a;
	}

	public LongList of(int size, long value) {
		return LongList.of(this.arrayOf(size, value));
	}

	// fill value
	public long[] zeros(int size) {
		return new long[size];
	}

	public LongList zerosArray(int size) {
		return LongList.of(this.zeros(size));
	}

	public LongList ones(int size) {
		return LongList.of(this.onesArray(size));
	}

	public long[] onesArray(int size) {
		long[] a = new long[size];
		Arrays.fill(a, 1);
		return a;
	}

	// ranges

	public LongList range(LongRange range) {
		return LongList.of(rangeArray(range));
	}

	public long[] rangeArray(LongRange range) {
		return range.stream().toArray();
	}

	public LongList range(long start, long end, long step) {
		return this.range(LongRange.of(start, end, step));
	}

	public long[] rangeArray(long start, long end, long step) {
		return LongRange.of(start, end, step).stream().toArray();
	}

	public LongList range(long end) {
		return this.range(LongRange.of(end));
	}

	public long[] rangeArray(long end) {
		return LongRange.of(end).stream().toArray();
	}

	public LongList range(long start, long end) {
		return this.range(LongRange.of(start, end));
	}

	public long[] rangeArray(long start, long end) {
		return LongRange.of(start, end).stream().toArray();
	}

	// random
	public long[] randomArray(int size) {
		return random.longs(size).toArray();
	}

	public LongList random(int size) {
		return LongList.of(this.randomArray(size));
	}

	public long[] randomArray(int size, long from, long to) {
		return random.longs(size, from, to).toArray();
	}

	public LongList random(int size, long from, long to) {
		return LongList.of(this.randomArray(size, from, to));
	}

	public long[] randomArray(long seed, int size) {
		Random r = new Random(seed);
		return r.longs(size).toArray();
	}

	public LongList random(long seed, int size) {
		return LongList.of(this.randomArray(seed, size));
	}

	public long[] randomArray(long seed, int size, long from, long to) {
		Random r = new Random(seed);
		return r.longs(size, from, to).toArray();
	}

	public LongList random(long seed, int size, long from, long to) {
		return LongList.of(this.randomArray(seed, size, from, to));
	}

	public long[] randomNormalArray(long seed, int size, long mean, long std) {
		Random r = new Random(seed);

		long[] a = new long[size];
		for (int i = 0; i < size; i++) {
			a[i] = (int) (r.nextGaussian() * std + mean);
		}

		return a;
	}

	public LongList randomNormal(long seed, int size, long mean, long std) {
		return LongList.of(this.randomNormalArray(seed, size, mean, std));
	}

	public LongList randomNormal(long seed, int size) {
		return LongList.of(
				this.randomNormalArray(seed, size, random.nextInt(Short.MAX_VALUE), random.nextInt(Short.MAX_VALUE)));
	}

	public long[] randomNormalArray(int size, long mean, long std) {
		long[] a = new long[size];
		for (int i = 0; i < size; i++) {
			a[i] = (int) (this.random.nextGaussian() * std + mean);
		}
		return a;
	}

	public LongList randomNormal(int size, long mean, long std) {
		return LongList.of(this.randomNormalArray(size, mean, std));
	}

	public LongList randomNormal(int size) {
		return LongList
				.of(this.randomNormalArray(size, random.nextInt(Short.MAX_VALUE), random.nextInt(Short.MAX_VALUE)));
	}

	// supplied
	public LongList generate(LongStream ts) {
		return LongList.of(ts.toArray());
	}

	public LongList generate(int size, LongStream ts) {
		return LongList.of(ts.limit(size).toArray());
	}

	public LongList generate(int size, LongSupplier ts) {
		return LongList.of(LongStream.generate(ts).limit(size).toArray());
	}
}
