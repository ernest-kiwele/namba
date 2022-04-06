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

package io.namba.arrays.agg;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.namba.arrays.DataList;
import io.namba.arrays.Table;
import io.namba.arrays.data.tuple.Two;
import io.namba.functions.LongMath;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class LongGrouping<K> extends ObjectGrouping<K, Long> {

	protected LongGrouping(DataList<Long> handle, Map<K, List<Integer>> groups, boolean binned) {
		super(handle, groups, binned);
	}

	public static <K> LongGrouping<K> of(DataList<Long> handle, Map<K, List<Integer>> groups) {
		return new LongGrouping<>(handle, groups, false);
	}

	public long sum(K key) {
		return this.reduce(key, (a, b) -> a + b).get(); // should never be empty, right?
	}

	public Map<K, Long> sum() {
		return this.reduce((a, b) -> a + b);
	}

	public Table sumTable() {
		return Table.of(this.sum());
	}

	public double mean(K key) {
		return this.groups.getOrDefault(key, Collections.emptyList()).stream().mapToLong(this.handle::getAt).average()
				.getAsDouble();
	}

	public Map<K, Double> mean() {
		return this.groups.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> this.mean(e.getKey())));
	}

	public Table meanTable() {
		return Table.of(this.sum());
	}

	public long prod(K key) {
		return this.reduce(key, 1l, (a, b) -> a * b);
	}

	public Map<K, Long> prod() {
		return this.reduce((a, b) -> a * b);
	}

	public Table prodTable() {
		return Table.of(this.sum());
	}

	public double median(K key) {
		return LongMath.median(this.groups.getOrDefault(key, Collections.emptyList()).stream().map(this.handle::getAt)
				.collect(Collectors.toList()));
	}

	public Map<K, Double> median() {
		return this.groups.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> this.median(e.getKey())));
	}

	public Table medianTable() {
		return Table.of(this.sum());
	}

	public Map<K, List<Integer>> groupIndices() {
		return Collections.unmodifiableMap(this.groups);
	}

	public void forEachGroup(BiConsumer<K, Collection<Long>> processor) {
		this.groups.entrySet().stream().map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue())))
				.forEach(two -> processor.accept(two.a(), two.b().values()));
	}

	public Stream<Two<K, Collection<Long>>> groupStream() {
		return this.groups.entrySet().stream().map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue()).values()));
	}

	public Stream<Two<K, Collection<Long>>> stream() {
		return this.groupStream();
	}

	public Map<K, Long> max() {
		return this.max(Comparator.naturalOrder());
	}

	public Map<K, Long> min() {
		return this.min(Comparator.naturalOrder());
	}

	public Table maxTable() {
		return Table.of(this.max(Comparator.naturalOrder()));
	}

	public Table minTable() {
		return Table.of(this.min(Comparator.naturalOrder()));
	}
}
