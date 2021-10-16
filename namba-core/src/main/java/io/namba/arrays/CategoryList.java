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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.namba.arrays.data.tuple.Three;
import io.namba.arrays.data.tuple.Two;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class CategoryList implements NambaList {

	private final String[] levels;
	private final int[] value;
	private final Map<Integer, String> mapping;

	protected CategoryList(List<String> is) {
		Three<String[], int[], Map<Integer, String>> map = toCategory(is);

		this.levels = map.a();
		this.value = map.b();
		this.mapping = map.c();
	}

	protected CategoryList(String[] levels, int[] value, Map<Integer, String> mapping) {
		this.levels = levels;
		this.value = value;
		this.mapping = mapping;
	}

	public static CategoryList of(StringList sl) {
		return new CategoryList(sl.value);
	}

	private static Three<String[], int[], Map<Integer, String>> toCategory(List<String> list) {
		Map<String, Integer> cats = new HashMap<>();
		int[] res = new int[list.size()];
		int[] v = { 0 };

		for (int i = 0; i < list.size(); i++) {
			String s = list.get(i);
			int position = cats.computeIfAbsent(s, a -> v[0]++);
			res[i] = position;
		}

		String[] levels = cats.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey)
				.toArray(i -> new String[i]);
		Map<Integer, String> levelMapping = cats.entrySet().stream().map(vv -> Two.of(vv.getValue(), vv.getKey()))
				.collect(Collectors.toMap(Two::a, Two::b));

		return Three.of(levels, res, levelMapping);
	}

	@Override
	public DataType dataType() {
		return DataType.CATEGORY;
	}

	@Override
	public Index index() {
		return null;
	}

	@Override
	public CategoryList repeat(int n) {
		int[] v = new int[n * this.value.length];

		for (int i = 0; i < n; i++) {
			System.arraycopy(this.value, 0, v, i * this.value.length, this.value.length);
		}

		return new CategoryList(this.levels, v, this.mapping);
	}

	@Override
	public int size() {
		return this.value.length;
	}

	@Override
	public StringList string() {
		return StringList.of(Arrays.stream(this.value).mapToObj(this.mapping::get).collect(Collectors.toList()));
	}

	@Override
	public StringList getAt(int[] loc) {
		return StringList.of(Arrays.stream(loc).mapToObj(i -> mapping.get(this.value[i])).collect(Collectors.toList()));
	}

	public CategoryList addCategories(String... newCategories) {
		if (null == newCategories || 0 == newCategories.length) {
			return this;
		}

		List<String> all = new ArrayList<>(Arrays.asList(this.levels));
		all.addAll(Arrays.asList(newCategories));

		Three<String[], int[], Map<Integer, String>> th = toCategory(all);
		Map<Integer, String> newMap = new LinkedHashMap<>(this.mapping);
		newMap.putAll(th.c());

		return new CategoryList(th.a(), this.value, newMap);
	}

	public CategoryList reorder(List<String> newOrder) {
		// TODO: implement
		return null;
	}

	public CategoryList sorted() {
		// TODO: implement
		return null;
	}

	public CategoryList sortedReverse() {
		// TODO: implement
		return null;
	}

	public List<String> levels() {
		return Arrays.asList(this.levels);
	}

	@Override
	public IntList asInt() {
		return null;
	}

	@Override
	public LongList asLong() {
		return null;
	}

	@Override
	public DoubleList asDouble() {
		return null;
	}

	@Override
	public Mask asMask() {
		return null;
	}

	// public CategoryList renameCategories(Map<String, String> nameMapping) {
	//
	// }
	//
	// public CategoryList renameCategory(String oldName, String newName) {
	//
	// }

}
