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
import java.util.stream.IntStream;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class Mask {

	private final boolean[] value;

	private Mask(boolean[] array) {
		this.value = array;
	}

	public static Mask of(boolean[] array) {
		return new Mask(array);
	}
	
	public static Mask trues(int size) {
		boolean[] b = new boolean[size];
		Arrays.fill(b, true);
		return new Mask(b);
	}
	
	public static Mask falses(int size) {
		boolean[] b = new boolean[size];
		Arrays.fill(b, false); 
		return new Mask(b);
	}

	public static Mask of(Boolean[] array) {
		boolean[] b = new boolean[array.length];
		for (int i = 0; i < array.length; i++)
			b[i] = array[i];

		return new Mask(b);
	}

	@Override
	public String toString() {
		return Arrays.toString(this.value);
	}

	/**
	 * Count true values
	 * 
	 * @return
	 */
	public int count() {
		return (int) IntStream.range(0, this.value.length).filter(i -> this.value[i]).count();
	}
	
	public int trueCount() {
		return this.count();
	}

	/**
	 * Count false values
	 * 
	 * @return
	 */
	public int falseCount() {
		return this.value.length - this.count();
	}

	public IntList truthy() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> this.value[i]).toArray());
	}

	public IntList falsy() {
		return IntList.of(IntStream.range(0, this.value.length).filter(i -> !this.value[i]).toArray());
	}

	public boolean all() {
		return IntStream.range(0, this.value.length).allMatch(i -> this.value[i]);
	}

	public boolean any() {
		return IntStream.range(0, this.value.length).anyMatch(i -> this.value[i]);
	}

	public boolean none() {
		return IntStream.range(0, this.value.length).noneMatch(i -> this.value[i]);
	}

	public boolean anyFalse() {
		return IntStream.range(0, this.value.length).anyMatch(i -> !this.value[i]);
	}
}
