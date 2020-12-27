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

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * 
 * @author Ernest Kiwele
 */
public class DoubleList implements NambaList {

	protected final double[] value;
	protected final Index index;

	private DoubleList(double[] v) {
		this.value = v;
		this.index = null;
	}

	private DoubleList(double[] v, Index index) {
		this.value = v;
		this.index = index;
	}

	@Override
	public final int size() {
		return this.value.length;
	}

	@Override
	public final DataType dataType() {
		return DataType.DOUBLE;
	}

	@Override
	public Index index() {
		return this.index;
	}

	public static DoubleList of(double[] v) {
		return new DoubleList(v);
	}

	public static DoubleList of(int[] v) {
		return new DoubleList(IntStream.of(v).mapToDouble(i -> i).toArray());
	}

	public static DoubleList of(long[] v) {
		return new DoubleList(LongStream.of(v).mapToDouble(i -> i).toArray());
	}

	@Override
	public NambaList getAt(int[] loc) {
		return DoubleList.of(IntStream.of(loc).mapToDouble(i -> this.value[i]).toArray());
	}

	public double getAt(int loc) {
		return this.value[loc];
	}

	@Override
	public String toString() {
		return Arrays.toString(this.value);
	}

	@Override
	public StringList string() {
		return StringList.of(DoubleStream.of(value).mapToObj(Double::toString).collect(Collectors.toList()));
	}

	public DoubleStream stream() {
		return Arrays.stream(value);
	}

	// casting
	public IntList asInt() {
		return ListCast.toInt(this);
	}
	
	public DecimalList asDecimal() {
		return ListCast.toDecimal(this);
	}

	public LongList asLong() {
		return ListCast.toLong(this);
	}

	public DataList<Double> boxed() {
		return ListCast.boxed(this);
	}

	public DataList<Double> toDouble() {
		return this.boxed();
	}

	// masks
	public Mask isnan() {
		boolean[] b = new boolean[this.value.length];

		for (int i = 0; i < this.value.length; i++) {
			b[i] = Double.isNaN(this.value[i]);
		}

		return Mask.of(b);
	}

	// impl
	@Override
	public DoubleList repeat(int n) {
		double[] v = new double[n * this.value.length];

		for (int i = 0; i < n; i++) {
			System.arraycopy(this.value, 0, v, i * this.value.length, this.value.length);
		}

		return DoubleList.of(v);
	}

	/**
	 * Returns indices of non-zero elements
	 * 
	 * @return
	 */
	public IntList nonZero() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> this.value[i] != 0.0).toArray());
	}

	public LongList round() {
		return LongList.of(Arrays.stream(this.value).mapToLong(Math::round).toArray());
	}

	public DoubleList invert() {
		return DoubleList.of(Arrays.stream(this.value).map(i -> 1 / i).toArray());
	}
}
