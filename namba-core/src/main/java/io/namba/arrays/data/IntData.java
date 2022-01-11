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
import java.util.function.IntSupplier;
import java.util.stream.IntStream;

import io.namba.arrays.IntList;
import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 */
public class IntData {

	private static final IntData instance = new IntData();
	private final Random random = new Random();

	private IntData() {
	}

	public static IntData instance() {
		return instance;
	}

	public static IntData seed(long seed) {
		IntData d = new IntData();
		d.random.setSeed(seed);
		return d;
	}

	public int[] arrayOf(int size, int value) {
		int[] a = new int[size];
		Arrays.fill(a, value);
		return a;
	}

	public IntList of(int size, int value) {
		return IntList.of(this.arrayOf(size, value));
	}

	// fill value
	public int[] zeros(int size) {
		return new int[size];
	}

	public IntList zerosArray(int size) {
		return IntList.of(this.zeros(size));
	}

	public IntList ones(int size) {
		return IntList.of(this.onesArray(size));
	}

	public int[] onesArray(int size) {
		int[] a = new int[size];
		Arrays.fill(a, 1);
		return a;
	}

	// ranges

	public IntList range(IntRange range) {
		return IntList.of(rangeArray(range));
	}

	public int[] rangeArray(IntRange range) {
		return range.stream().toArray();
	}

	public IntList range(int start, int end, int step) {
		return this.range(IntRange.of(start, end, step));
	}

	public int[] rangeArray(int start, int end, int step) {
		return IntRange.of(start, end, step).stream().toArray();
	}

	public IntList range(int end) {
		return this.range(IntRange.of(end));
	}

	public int[] rangeArray(int end) {
		return IntRange.of(end).stream().toArray();
	}

	public IntList range(int start, int end) {
		return this.range(IntRange.of(start, end));
	}

	public int[] rangeArray(int start, int end) {
		return IntRange.of(start, end).stream().toArray();
	}

	// random
	public int[] randomArray(int size) {
		return random.ints(size).toArray();
	}

	public IntList random(int size) {
		return IntList.of(this.randomArray(size));
	}

	public int[] randomArray(int size, int from, int to) {
		return random.ints(size, from, to).toArray();
	}

	public IntList random(int size, int from, int to) {
		return IntList.of(this.randomArray(size, from, to));
	}

	public int[] randomArray(long seed, int size) {
		Random r = new Random(seed);
		return r.ints(size).toArray();
	}

	public IntList random(long seed, int size) {
		return IntList.of(this.randomArray(seed, size));
	}

	public int[] randomArray(long seed, int size, int from, int to) {
		Random r = new Random(seed);
		return r.ints(size, from, to).toArray();
	}

	public IntList random(long seed, int size, int from, int to) {
		return IntList.of(this.randomArray(seed, size, from, to));
	}

	public int[] randomNormalArray(long seed, int size, int mean, int std) {
		Random r = new Random(seed);

		int[] a = new int[size];
		for (int i = 0; i < size; i++) {
			a[i] = (int) (r.nextGaussian() * std + mean);
		}

		return a;
	}

	public IntList randomNormal(long seed, int size, int mean, int std) {
		return IntList.of(this.randomNormalArray(seed, size, mean, std));
	}

	public IntList randomNormal(long seed, int size) {
		return IntList.of(
				this.randomNormalArray(seed, size, random.nextInt(Short.MAX_VALUE), random.nextInt(Short.MAX_VALUE)));
	}

	public int[] randomNormalArray(int size, int mean, int std) {
		int[] a = new int[size];
		for (int i = 0; i < size; i++) {
			a[i] = (int) (this.random.nextGaussian() * std + mean);
		}
		return a;
	}

	public IntList randomNormal(int size, int mean, int std) {
		return IntList.of(this.randomNormalArray(size, mean, std));
	}

	public IntList randomNormal(int size) {
		return IntList
				.of(this.randomNormalArray(size, random.nextInt(Short.MAX_VALUE), random.nextInt(Short.MAX_VALUE)));
	}

	// supplied
	public IntList generate(IntStream ts) {
		return IntList.of(ts.toArray());
	}

	public IntList generate(int size, IntStream ts) {
		return IntList.of(ts.limit(size).toArray());
	}

	public IntList generate(int size, IntSupplier ts) {
		return IntList.of(IntStream.generate(ts).limit(size).toArray());
	}
}
