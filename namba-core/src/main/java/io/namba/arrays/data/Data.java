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

/**
 * 
 * @author Ernest Kiwele
 */
public class Data {
	private static final Data instance = new Data();

	public final IntData ints = IntData.instance();
	public final LongData longs = LongData.instance();
	public final DoubleData doubles = DoubleData.instance();
	public final DecimalData decimals = DecimalData.instance();

	private Data() {
	}

	public static Data instance() {
		return instance;
	}
}
