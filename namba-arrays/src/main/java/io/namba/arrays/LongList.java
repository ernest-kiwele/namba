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
import java.util.stream.LongStream;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class LongList {

	protected final long[] value;

	private LongList(long[] a) {
		this.value = a;
	}

	public static LongList of(long[] a) {
		return new LongList(a);
	}

	public LongStream stream() {
		return Arrays.stream(this.value);
	}

	public DecimalList asDecimal() {
		return ListCast.toDecimal(this);
	}

	public DoubleList asDouble() {
		return ListCast.toDouble(this);
	}

	public IntList asInt() {
		return ListCast.toInt(this);
	}
}
