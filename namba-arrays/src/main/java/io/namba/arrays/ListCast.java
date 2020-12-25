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
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * 
 * @author Ernest Kiwele
 */
public class ListCast {

	private ListCast() {
	}

	public static DoubleList toDouble(IntList l) {
		return DoubleList.of(Arrays.stream(l.value).mapToDouble(i -> i).toArray());
	}

	public static DoubleList toDouble(LongList l) {
		return DoubleList.of(Arrays.stream(l.value).mapToDouble(i -> i).toArray());
	}

	public static LongList toLong(IntList l) {
		return LongList.of(Arrays.stream(l.value).mapToLong(i -> i).toArray());
	}

	public static LongList toLong(DoubleList l) {
		return LongList.of(Arrays.stream(l.value).mapToLong(i -> (long) i).toArray());
	}

	public static IntList toInt(LongList l) {
		return IntList.of(Arrays.stream(l.value).mapToInt(i -> (int) i).toArray());
	}

	public static IntList toInt(DoubleList l) {
		return IntList.of(Arrays.stream(l.value).mapToInt(i -> (int) i).toArray());
	}

	public static <T> DoubleList toDouble(DataList<T> l, ToDoubleFunction<T> func) {
		return DoubleList.of(l.stream().mapToDouble(func::applyAsDouble).toArray());
	}

	public static <T> IntList toInt(DataList<T> l, ToIntFunction<T> func) {
		return IntList.of(l.stream().mapToInt(func::applyAsInt).toArray());
	}

	public static <T> LongList toLong(DataList<T> l, ToLongFunction<T> func) {
		return LongList.of(l.stream().mapToLong(func::applyAsLong).toArray());
	}

	public static DataList<Integer> boxed(IntList l) {
		return DataList.of(DataType.INT, l.stream().boxed().toArray(i -> new Integer[0]));
	}

	public static DataList<Long> boxed(LongList l) {
		return DataList.of(DataType.LONG, l.stream().boxed().toArray(i -> new Long[0]));
	}

	public static DataList<Double> boxed(DoubleList l) {
		return DataList.of(DataType.DOUBLE, l.stream().boxed().toArray(i -> new Double[0]));
	}
}
