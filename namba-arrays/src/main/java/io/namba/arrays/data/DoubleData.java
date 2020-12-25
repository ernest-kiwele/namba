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
import java.util.function.DoubleSupplier;
import java.util.stream.DoubleStream;

import io.namba.arrays.DoubleList;
import io.namba.arrays.Mask;
import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class DoubleData {
	private static final DoubleData instance = new DoubleData();
	private final Random random = new Random();

	private DoubleData() {
	}

	public static DoubleData instance() {
		return instance;
	}

	public static DoubleData seed(long seed) {
		DoubleData d = new DoubleData();
		d.random.setSeed(seed);
		return d;
	}

	public double[] arrayOf(int size, double value) {
		double[] a = new double[size];
		Arrays.fill(a, value);
		return a;
	}

	public DoubleList of(int size, int value) {
		return DoubleList.of(this.arrayOf(size, value));
	}

	// fill value
	public double[] zeros(int size) {
		return new double[size];
	}

	public DoubleList zerosArray(int size) {
		return DoubleList.of(this.zeros(size));
	}

	public DoubleList ones(int size) {
		return DoubleList.of(this.onesArray(size));
	}

	public double[] onesArray(int size) {
		double[] a = new double[size];
		Arrays.fill(a, 1);
		return a;
	}

	public DoubleList ones(int size, double fillValue) {
		return DoubleList.of(this.fullArray(size, fillValue));
	}

	public double[] fullArray(int size, double fillValue) {
		double[] a = new double[size];
		Arrays.fill(a, fillValue);
		return a;
	}

	public DoubleList nans(int size) {
		return DoubleList.of(this.nansArray(size));
	}

	public double[] nansArray(int size) {
		double[] a = new double[size];
		Arrays.fill(a, Double.NaN);
		return a;
	}

	// ranges

	public DoubleList range(IntRange range) {
		return DoubleList.of(rangeArray(range));
	}

	public double[] rangeArray(IntRange range) {
		return range.stream().mapToDouble(i -> i).toArray();
	}

	public DoubleList range(int start, int end, int step) {
		return this.range(IntRange.of(start, end, step));
	}

	public double[] rangeArray(int start, int end, int step) {
		return IntRange.of(start, end, step).stream().mapToDouble(i -> i).toArray();
	}

	public DoubleList range(int end) {
		return this.range(IntRange.of(end));
	}

	public double[] rangeArray(int end) {
		return IntRange.of(end).stream().mapToDouble(i -> i).toArray();
	}

	public DoubleList range(int start, int end) {
		return this.range(IntRange.of(start, end));
	}

	public double[] rangeArray(int start, int end) {
		return IntRange.of(start, end).stream().mapToDouble(i -> i).toArray();
	}

	// random
	public double[] randomArray(int size) {
		return random.doubles(size).toArray();
	}

	public DoubleList random(int size) {
		return DoubleList.of(this.randomArray(size));
	}

	public double[] randomArray(int size, double from, double to) {
		return random.doubles(size, from, to).toArray();
	}

	public DoubleList random(int size, double from, double to) {
		return DoubleList.of(this.randomArray(size, from, to));
	}

	public double[] randomArray(long seed, int size) {
		Random r = new Random(seed);
		return r.doubles(size).toArray();
	}

	public DoubleList random(long seed, int size) {
		return DoubleList.of(this.randomArray(seed, size));
	}

	public double[] randomArray(long seed, int size, int from, int to) {
		Random r = new Random(seed);
		return r.doubles(size, from, to).toArray();
	}

	public DoubleList random(long seed, int size, int from, int to) {
		return DoubleList.of(this.randomArray(seed, size, from, to));
	}

	public double[] randomNormalArray(long seed, int size, double mean, double std) {
		Random r = new Random(seed);

		double[] a = new double[size];
		for (int i = 0; i < size; i++) {
			a[i] = r.nextGaussian() * std + mean;
		}

		return a;
	}

	public DoubleList randomNormal(long seed, int size, double mean, double std) {
		return DoubleList.of(this.randomNormalArray(seed, size, mean, std));
	}

	public DoubleList randomNormal(long seed, int size) {
		return DoubleList.of(this.randomNormalArray(seed, size, random.nextInt(), random.nextInt()));
	}

	public double[] randomNormalArray(int size, double mean, double std) {
		double[] a = new double[size];
		for (int i = 0; i < size; i++) {
			a[i] = this.random.nextGaussian() * std + mean;
		}
		return a;
	}

	public DoubleList randomNormal(int size, int mean, int std) {
		return DoubleList.of(this.randomNormalArray(size, mean, std));
	}

	public DoubleList randomNormal(int size) {
		return DoubleList.of(this.randomNormalArray(size, random.nextInt(), random.nextInt()));
	}

	// supplied
	public DoubleList generate(DoubleStream ts) {
		return DoubleList.of(ts.toArray());
	}

	public DoubleList generate(int size, DoubleStream ts) {
		return DoubleList.of(ts.limit(size).toArray());
	}

	public DoubleList generate(int size, DoubleSupplier ts) {
		return DoubleList.of(DoubleStream.generate(ts).limit(size).toArray());
	}

	public DoubleList linSpace(int size, double from, double to, boolean endIncluded) {
		double r = (to - from) / (endIncluded ? size - 1 : size);
		return DoubleList.of(DoubleStream.iterate(from, v -> v + r).limit(size).toArray());
	}

	public DoubleList linSpace(int size, double from, double to) {
		return this.linSpace(size, from, to, true);
	}
	
	public DoubleList geomSpace(int size, double from, double to, boolean endIncluded) {
		double r = Math.pow((to / from), 1.0 / (endIncluded ? size - 1 : size));
		return DoubleList.of(DoubleStream.iterate(from, v -> v * r).limit(size).toArray());
	}

	public DoubleList geomSpace(int size, double from, double to) {
		return this.geomSpace(size, from, to, true);
	}
}
