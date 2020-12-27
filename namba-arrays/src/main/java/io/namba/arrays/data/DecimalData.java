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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.namba.arrays.DecimalList;
import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class DecimalData {

	private static final DecimalData instance = new DecimalData();
	private final Random random = new Random();

	private DecimalData() {
	}

	public static DecimalData instance() {
		return instance;
	}

	public static DecimalData seed(long seed) {
		DecimalData d = new DecimalData();
		d.random.setSeed(seed);
		return d;
	}

	public BigDecimal[] arrayOf(int size, BigDecimal value) {
		BigDecimal[] a = new BigDecimal[size];
		Arrays.fill(a, value);
		return a;
	}

	// paste
	public DecimalList of(int size, BigDecimal value) {
		return DecimalList.of(this.arrayOf(size, value));
	}

	// fill value
	public BigDecimal[] zerosArray(int size) {
		BigDecimal[] v = new BigDecimal[size];

		Arrays.fill(v, BigDecimal.ZERO);

		return v;
	}

	public DecimalList zeros(int size) {
		return DecimalList.of(this.zerosArray(size));
	}

	public DecimalList ones(int size) {
		return DecimalList.of(this.onesArray(size));
	}

	public BigDecimal[] onesArray(int size) {
		BigDecimal[] a = new BigDecimal[size];
		Arrays.fill(a, BigDecimal.ONE);
		return a;
	}

	public DecimalList ones(int size, BigDecimal fillValue) {
		return DecimalList.of(this.fullArray(size, fillValue));
	}

	public BigDecimal[] fullArray(int size, BigDecimal fillValue) {
		BigDecimal[] a = new BigDecimal[size];
		Arrays.fill(a, fillValue);
		return a;
	}

	public DecimalList nans(int size) {
		return DecimalList.of(this.nansArray(size));
	}

	public BigDecimal[] nansArray(int size) {
		return new BigDecimal[size];
	}

	// ranges
	public DecimalList range(IntRange range) {
		return DecimalList.of(rangeArray(range));
	}

	public BigDecimal[] rangeArray(IntRange range) {
		return range.stream().mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList range(int start, int end, int step) {
		return this.range(IntRange.of(start, end, step));
	}

	public BigDecimal[] rangeArray(int start, int end, int step) {
		return IntRange.of(start, end, step).stream().mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList range(int end) {
		return this.range(IntRange.of(end));
	}

	public BigDecimal[] rangeArray(int end) {
		return IntRange.of(end).stream().mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList range(int start, int end) {
		return this.range(IntRange.of(start, end));
	}

	public BigDecimal[] rangeArray(int start, int end) {
		return IntRange.of(start, end).stream().mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	// random
	public BigDecimal[] randomArray(int size) {
		return random.doubles(size).mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList random(int size) {
		return DecimalList.of(this.randomArray(size));
	}

	public BigDecimal[] randomArray(int size, double from, double to) {
		return random.doubles(size, from, to).mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList random(int size, double from, double to) {
		return DecimalList.of(this.randomArray(size, from, to));
	}

	public BigDecimal[] randomArray(long seed, int size) {
		Random r = new Random(seed);
		return r.doubles(size).mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList random(long seed, int size) {
		return DecimalList.of(this.randomArray(seed, size));
	}

	public BigDecimal[] randomArray(long seed, int size, double from, double to) {
		Random r = new Random(seed);
		return r.doubles(size, from, to).mapToObj(BigDecimal::valueOf).toArray(i -> new BigDecimal[i]);
	}

	public DecimalList random(long seed, int size, double from, double to) {
		return DecimalList.of(this.randomArray(seed, size, from, to));
	}

	public BigDecimal[] randomNormalArray(long seed, int size, BigDecimal mean, BigDecimal std) {
		Random r = new Random(seed);

		BigDecimal[] a = new BigDecimal[size];
		for (int i = 0; i < size; i++) {
			a[i] = BigDecimal.valueOf(r.nextGaussian()).multiply(std).add(mean);
		}

		return a;
	}

	public DecimalList randomNormal(long seed, int size, BigDecimal mean, BigDecimal std) {
		return DecimalList.of(this.randomNormalArray(seed, size, mean, std));
	}

	public DecimalList randomNormal(long seed, int size) {
		return DecimalList.of(this.randomNormalArray(seed, size, BigDecimal.valueOf(random.nextDouble()),
				BigDecimal.valueOf(random.nextDouble())));
	}

	public BigDecimal[] randomNormalArray(int size, BigDecimal mean, BigDecimal std) {
		BigDecimal[] a = new BigDecimal[size];
		for (int i = 0; i < size; i++) {
			a[i] = BigDecimal.valueOf(this.random.nextGaussian()).multiply(std).add(mean);
		}
		return a;
	}

	public DecimalList randomNormal(int size, BigDecimal mean, BigDecimal std) {
		return DecimalList.of(this.randomNormalArray(size, mean, std));
	}

	public DecimalList randomNormal(int size) {
		return DecimalList.of(this.randomNormalArray(size, BigDecimal.valueOf(random.nextDouble()),
				BigDecimal.valueOf(random.nextInt())));
	}

	// supplied
	public DecimalList generate(Stream<BigDecimal> ts) {
		return DecimalList.of(ts.toArray(i -> new BigDecimal[i]));
	}

	public DecimalList generate(int size, Stream<BigDecimal> ts) {
		return DecimalList.of(ts.limit(size).toArray(i -> new BigDecimal[i]));
	}

	public DecimalList generate(int size, Supplier<BigDecimal> ts) {
		return DecimalList.of(Stream.generate(ts).limit(size).toArray(i -> new BigDecimal[i]));
	}

	public DecimalList linSpace(int size, BigDecimal from, BigDecimal to, boolean endIncluded) {
		BigDecimal r = to.subtract(from).divide(BigDecimal.valueOf(endIncluded ? size - 1 : size));
		return DecimalList.of(Stream.iterate(from, v -> v.add(r)).limit(size).toArray(i -> new BigDecimal[i]));
	}

	public DecimalList linSpace(int size, BigDecimal from, BigDecimal to) {
		return this.linSpace(size, from, to, true);
	}

	public DecimalList linSpace(int size, double from, double to, boolean endIncluded) {
		BigDecimal fr = BigDecimal.valueOf(from);
		BigDecimal t = BigDecimal.valueOf(to);

		BigDecimal r = t.subtract(fr).divide(BigDecimal.valueOf(endIncluded ? size - 1 : size));
		return DecimalList.of(Stream.iterate(fr, v -> v.add(r)).limit(size).toArray(i -> new BigDecimal[i]));
	}

	public DecimalList linSpace(int size, double from, double to) {
		return this.linSpace(size, from, to, true);
	}

	/*
	 * Using double for from and to because can't calculate power using big decimal
	 */
	public DecimalList geomSpace(int size, double from, double to, boolean endIncluded) {
		BigDecimal r = BigDecimal.valueOf(Math.pow((to / from), 1.0 / (endIncluded ? size - 1 : size)));
		return DecimalList.of(Stream.iterate(BigDecimal.valueOf(from), v -> v.multiply(r)).limit(size)
				.toArray(i -> new BigDecimal[i]));
	}

	public DecimalList geomSpace(int size, double from, double to) {
		return this.geomSpace(size, from, to, true);
	}
}
