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
import java.util.Objects;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class IndexLevel {
	private final String name;
	private final Object key;
	private final int[] indices;
	
	private IndexLevel(String name, Object key, int[] indices) {
		this.name = name;
		this.key = Objects.requireNonNull(key, "key may not be null");
		this.indices = Objects.requireNonNull(indices, "indices may not be null");
	}

	public static IndexLevel of(String name, Object key, int[] indices) {
		return new IndexLevel(name, key, indices);
	}

	public static IndexLevel of(Object key, int[] indices) {
		return new IndexLevel(null, key, indices);
	}

	public String getName() {
		return name;
	}

	public Object getKey() {
		return key;
	}

	public int[] getIndices() {
		return indices;
	}

	@Override
	public String toString() {
		return "IndexLevel [name=" + name + ", key=" + key + ", indices=" + Arrays.toString(indices) + "]";
	}
	
}
