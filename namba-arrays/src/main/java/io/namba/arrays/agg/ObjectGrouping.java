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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.namba.arrays.DataList;
import io.namba.arrays.Table;
import io.namba.arrays.data.tuple.Two;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class ObjectGrouping<K, V> implements Grouping {

	protected final DataList<V> handle;
	protected final Map<K, List<Integer>> groups;
	protected final boolean binned;

	protected ObjectGrouping(DataList<V> handle, Map<K, List<Integer>> groups, boolean binned) {
		this.handle = handle;
		this.groups = groups;
		this.binned = binned;
	}

	public static <K, V> ObjectGrouping<K, V> of(DataList<V> handle, Map<K, List<Integer>> groups, boolean binned) {
		return new ObjectGrouping<>(handle, groups, binned);
	}

	public static <K, V> ObjectGrouping<K, V> ofBins(DataList<V> handle, Map<K, List<Integer>> groups) {
		return new ObjectGrouping<>(handle, groups, true);
	}

	public static <K, V> ObjectGrouping<K, V> ofClasses(DataList<V> handle, Map<K, List<Integer>> groups) {
		return new ObjectGrouping<>(handle, groups, false);
	}

	// aggregation
	public Optional<V> reduce(K key, BinaryOperator<V> reducer) {
		return this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).stream().reduce(reducer);
	}

	public Optional<V> agg(K key, BinaryOperator<V> reducer) {
		return this.reduce(key, reducer);
	}

	public V reduce(K key, V identity, BinaryOperator<V> reducer) {
		return this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).stream().reduce(identity,
				reducer);
	}

	public V agg(K key, V identity, BinaryOperator<V> reducer) {
		return this.reduce(key, identity, reducer);
	}

	public Map<K, V> reduce(BinaryOperator<V> reducer) {
		return this.groups.entrySet().stream()
				.map(e -> Two.of(e.getKey(), this.reduce(e.getKey(), reducer).orElse(null)))
				.collect(Collectors.toMap(Two::a, Two::b));
	}

	public Map<K, V> agg(BinaryOperator<V> reducer) {
		return this.reduce(reducer);
	}

	public Table reduceTable(BinaryOperator<V> reducer) {
		return Table.of(this.reduce(reducer));
	}

	public <U> U mapReduce(K key, Function<V, U> mapper, BinaryOperator<U> reducer) {
		return this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).stream().map(mapper)
				.reduce(reducer).orElse(null);
	}

	public <U> U mapReduce(K key, Function<V, U> mapper, U identity, BinaryOperator<U> reducer) {
		return this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).stream().map(mapper)
				.reduce(identity, reducer);
	}

	public <U> Map<K, U> mapReduce(Function<V, U> mapper, BinaryOperator<U> reducer) {
		return this.groups.entrySet().stream()
				.map(e -> Two.of(e.getKey(),
						this.handle.getAt(e.getValue()).stream().map(mapper).reduce(reducer).orElse(null)))
				.collect(Two.mapCollector());
	}

	public <U> Table mapReduceTable(Function<V, U> mapper, BinaryOperator<U> reducer) {
		return Table.of(this.mapReduce(mapper, reducer));
	}

	public <U> Map<K, U> mapReduce(Function<V, U> mapper, U identity, BinaryOperator<U> reducer) {
		return this.groups.entrySet().stream().map(
				e -> Two.of(e.getKey(), this.handle.getAt(e.getValue()).stream().map(mapper).reduce(identity, reducer)))
				.collect(Two.mapCollector());
	}

	public <U> Table mapReduceTable(Function<V, U> mapper, U identity, BinaryOperator<U> reducer) {
		return Table.of(this.mapReduce(mapper, identity, reducer));
	}

	@Override
	public int groupCount() {
		return this.groups.size();
	}

	@Override
	public int depth() {
		return 1;
	}

	@Override
	public V first(Object key) {
		int loc = this.firstLoc(key);
		return loc < 0 ? null : this.handle.getAt(loc);
	}

	@Override
	public V last(Object key) {
		int loc = this.lastLoc(key);
		return loc < 0 ? null : this.handle.getAt(loc);
	}

	@Override
	public V nth(Object key, int n) {
		int loc = this.nthLoc(key, n);
		return loc < 0 ? null : this.handle.getAt(loc);
	}

	@Override
	public V nthLast(Object key, int n) {
		int loc = this.nthLastLoc(key, n);
		return loc < 0 ? null : this.handle.getAt(loc);
	}

	public Map<K, V> first() {
		return this.keys().stream().collect(Collectors.toMap(Function.identity(), this::first));
	}

	public Table firstTable() {
		return Table.of(this.first());
	}

	public Map<K, V> getFirst() {
		return this.first();
	}

	public Map<K, V> last() {
		return this.keys().stream().collect(Collectors.toMap(Function.identity(), this::last));
	}

	public Table lastTable() {
		return Table.of(this.last());
	}

	public Map<K, V> getLast() {
		return this.last();
	}

	public Map<K, Integer> count() {
		return this.groups.entrySet().stream()
				.collect(Collectors.groupingBy(Entry::getKey, Collectors.summingInt(e -> e.getValue().size())));
	}

	public Map<K, Integer> countUnique() {
		return this.groups.entrySet().stream().collect(Collectors.groupingBy(Entry::getKey,
				Collectors.summingInt(e -> (int) e.getValue().stream().map(handle::getAt).distinct().count())));
	}

	public Table countTable() {
		return Table.of(this.count());
	}

	@Override
	public int firstLoc(Object key) {
		List<Integer> indices = this.groups.get(key);
		if (null == indices || indices.isEmpty())
			return -1;
		return indices.get(0);
	}

	@Override
	public int lastLoc(Object key) {
		List<Integer> indices = this.groups.get(key);
		if (null == indices || indices.isEmpty())
			return -1;
		return indices.get(indices.size() - 1);
	}

	@Override
	public int nthLoc(Object key, int n) {
		List<Integer> indices = this.groups.get(key);
		if (null == indices || indices.isEmpty())
			return -1;
		return n >= indices.size() ? -1 : indices.get(n);
	}

	@Override
	public int nthLastLoc(Object key, int n) {
		List<Integer> indices = this.groups.get(key);
		if (null == indices || indices.isEmpty())
			return -1;
		return indices.size() - n - 1 < 0 ? -1 : indices.get(indices.size() - n);
	}

	public Set<Two<K, DataList<V>>> groups() {
		return this.groups.entrySet().stream().map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue())))
				.collect(Collectors.toUnmodifiableSet());
	}

	public Set<Two<K, DataList<V>>> getGroups() {
		return this.groups();
	}

	public Set<K> keys() {
		return this.groups.keySet();
	}

	public Set<K> getKeys() {
		return this.keys();
	}

	public Set<K> emptyKeys() {
		return this.groups.entrySet().stream().filter(e -> e.getValue().isEmpty()).map(Entry::getKey)
				.collect(Collectors.toUnmodifiableSet());
	}

	public Set<K> getEmptyKeys() {
		return this.emptyKeys();
	}

	public Map<K, Integer> hist() {
		return this.mapReduce(v -> 1, (a, b) -> a + b);
	}

	public Map<K, Integer> histogram() {
		return this.hist();
	}

	public Map<K, Integer> valueCount() {
		return this.hist();
	}

	public boolean all(K key, Predicate<V> test) {
		return groups.getOrDefault(key, Collections.emptyList()).stream().map(this.handle::getAt).allMatch(test);
	}

	public Map<K, Boolean> all(Predicate<V> tester) {
		return groups.entrySet().stream()
				.map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue()).stream().allMatch(tester)))
				.collect(Two.mapCollector());
	}

	public Table allTable(Predicate<V> test) {
		return Table.of(this.all(test));
	}

	public boolean any(K key, Predicate<V> test) {
		return groups.getOrDefault(key, Collections.emptyList()).stream().map(this.handle::getAt).anyMatch(test);
	}

	public Map<K, Boolean> any(Predicate<V> tester) {
		return groups.entrySet().stream()
				.map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue()).stream().anyMatch(tester)))
				.collect(Two.mapCollector());
	}

	public Table anyTable(Predicate<V> test) {
		return Table.of(this.all(test));
	}

	public Map<K, V> max(Comparator<V> comparator) {
		return this.groups.entrySet().stream()
				.map(entry -> Two.of(entry.getKey(),
						entry.getValue().stream().map(this.handle::getAt).max(comparator).orElse(null)))
				.collect(Two.mapCollector());
	}

	public Map<K, V> min(Comparator<V> comparator) {
		return this.groups.entrySet().stream()
				.map(entry -> Two.of(entry.getKey(),
						entry.getValue().stream().map(this.handle::getAt).max(comparator).orElse(null)))
				.collect(Two.mapCollector());
	}
}
