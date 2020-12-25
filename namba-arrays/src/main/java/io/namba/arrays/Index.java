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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;

/**
 * 
 * @author Ernest Kiwele
 */
public class Index {
	private static final int[] EMPTY_ARRAY = new int[0];
	protected final List<IndexLevel> levels;
	protected final Map<Object, IndexLevel> keyMap;

	protected Index(List<IndexLevel> levels) {
		this.levels = Collections.unmodifiableList(levels);
		this.keyMap = this.levels.stream().collect(Collectors.toMap(IndexLevel::getKey, Function.identity()));
	}

	public List<IndexLevel> getLevels() {
		return levels;
	}

	public int[] getByKey(Object o) {
		return Optional.ofNullable(this.keyMap.get(o)).map(IndexLevel::getIndices).orElse(EMPTY_ARRAY);
	}

	public static Index intIndex(int[] values, IntFunction<Object> indexer) {
		List<IndexLevel> levels= IntStream.range(0, values.length).mapToObj(i -> Pair.of(indexer.apply(values[i]), i))
				.collect(Collectors.groupingBy(Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors
						.collectingAndThen(Collectors.toList(), list -> list.stream().mapToInt(i -> i).toArray()))))
				.entrySet()
				.stream()
				.map(entry -> IndexLevel.of(entry.getKey(), entry.getValue()))
				.collect(Collectors.toList());
		
		return new Index(levels);
	}
	
	public int getSize() {
		return this.keyMap.size();
	}
	
	public Set<Object> getKeys() {
		return this.keyMap.keySet();
	}
	
	@Override
	public String toString() {
		return this.levels.toString();
	}
}
