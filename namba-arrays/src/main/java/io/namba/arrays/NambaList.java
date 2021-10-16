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

/**
 * 
 * @author Ernest Kiwele
 */
public interface NambaList {

	public static int SUMMARY_SIZE = 5;

	default String getName() {
		return null;
	}

	DataType dataType();

	default DataType getDataType() {
		return this.dataType();
	}

	int size();

	default int getSize() {
		return size();
	}

	Index index();

	default Index getIndex() {
		return index();
	}

	StringList string();

	NambaList getAt(int[] loc);

	NambaList repeat(int n);

	// casting
	/**
	 * Convert this array into an int list array. If the current data type is not
	 * compatible with integer, null is returned. If individual values are not
	 * compatible with integer, corresponding data is set to null.
	 * 
	 * @return null if array cannot be converted, conversion result otherwise.
	 */
	IntList asInt();

	LongList asLong();

	DoubleList asDouble();

	Mask asMask();
}
