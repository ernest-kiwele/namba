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

import java.math.BigDecimal;
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
import io.namba.arrays.DecimalList;
import io.namba.arrays.Table;
import io.namba.arrays.data.tuple.Two;
import io.namba.functions.NambaMath;

/**
 * 
 * @author Ernest Kiwele
 */
public class DecimalGrouping<K> extends ObjectGrouping<K, BigDecimal> {

	protected DecimalGrouping(DataList<BigDecimal> handle, Map<K, List<Integer>> groups, boolean binned) {
		super(handle, groups, binned);
	}

	public static <K> DecimalGrouping<K> of(DataList<BigDecimal> handle, Map<K, List<Integer>> groups) {
		return new DecimalGrouping<>(handle, groups, false);
	}

	// TODO: implement
	// public BigDecimal standardErrorMean(K key) {
	// return null;
	// }
	//
	// public BigDecimal sem(K key) {
	// return this.standardErrorMean(key);
	// }

	// TODO: implement
	// public Map<K, BigDecimal> standardErrorMean() {
	// return null;
	// }
	// public Map<K, BigDecimal> sem() {
	// return this.standardErrorMean();
	// }

	public BigDecimal sampleStd(K key) {
		return NambaMath.sampleStd(this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).values(),
				DecimalList.DEFAULT_MATH_CONTEXT);
	}

	public BigDecimal populationStd(K key) {
		return NambaMath.populationStd(
				this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).values(),
				DecimalList.DEFAULT_MATH_CONTEXT);
	}

	public BigDecimal std(K key) {
		return this.populationStd(key);
	}

	public Map<K, BigDecimal> populationStd() {
		return this.groups.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> this.populationStd(e.getKey())));
	}

	public Map<K, BigDecimal> sampleStd() {
		return this.groups.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> this.populationStd(e.getKey())));
	}

	public Map<K, BigDecimal> std() {
		return this.populationStd();
	}

	public Table populationStdTable() {
		return Table.of(this.std());
	}

	public Table sampleStdTable() {
		return Table.of(this.std());
	}

	public Table stdTable() {
		return Table.of(this.std());
	}

	public BigDecimal sum(K key) {
		return this.reduce(key, BigDecimal::add).orElse(null);
	}

	public Map<K, BigDecimal> sum() {
		return this.reduce(BigDecimal::add);
	}

	public Table sumTable() {
		return Table.of(this.sum());
	}

	public BigDecimal populationVar(K key) {
		return NambaMath.populationVar(
				this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).values(),
				DecimalList.DEFAULT_MATH_CONTEXT);
	}

	public Map<K, BigDecimal> populationVar() {
		return this.groups.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> this.populationVar(e.getKey())));
	}

	public BigDecimal sampleVar(K key) {
		return NambaMath.sampleVar(this.handle.getAt(this.groups.getOrDefault(key, Collections.emptyList())).values(),
				DecimalList.DEFAULT_MATH_CONTEXT);
	}

	public Map<K, BigDecimal> sampleVar() {
		return this.groups.entrySet().stream()
				.collect(Collectors.toMap(Entry::getKey, e -> this.sampleVar(e.getKey())));
	}

	public Table populationVarTable() {
		return Table.of(this.populationVar());
	}

	public Table sampleVarTable() {
		return Table.of(this.sampleVar());
	}

	public BigDecimal mean(K key) {
		return NambaMath.mean(this.groups.getOrDefault(key, Collections.emptyList()).stream().map(this.handle::getAt)
				.collect(Collectors.toList()));
	}

	public Map<K, BigDecimal> mean() {
		return this.groups.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> this.mean(e.getKey())));
	}

	public Table meanTable() {
		return Table.of(this.sum());
	}

	public BigDecimal prod(K key) {
		return this.reduce(key, BigDecimal.ONE, BigDecimal::multiply);
	}

	public Map<K, BigDecimal> prod() {
		return this.reduce(BigDecimal::multiply);
	}

	public Table prodTable() {
		return Table.of(this.sum());
	}

	public BigDecimal median(K key) {
		return NambaMath.median(this.groups.getOrDefault(key, Collections.emptyList()).stream().map(this.handle::getAt)
				.collect(Collectors.toList()));
	}

	public Map<K, BigDecimal> median() {
		return this.groups.entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> this.median(e.getKey())));
	}

	public Table medianTable() {
		return Table.of(this.sum());
	}

	public Map<K, List<Integer>> groupIndices() {
		return Collections.unmodifiableMap(this.groups);
	}

	public void forEachGroup(BiConsumer<K, Collection<BigDecimal>> processor) {
		this.groups.entrySet().stream().map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue())))
				.forEach(two -> processor.accept(two.a(), two.b().values()));
	}

	public Stream<Two<K, Collection<BigDecimal>>> groupStream() {
		return this.groups.entrySet().stream().map(e -> Two.of(e.getKey(), this.handle.getAt(e.getValue()).values()));
	}

	public Stream<Two<K, Collection<BigDecimal>>> stream() {
		return this.groupStream();
	}

	public Map<K, BigDecimal> max() {
		return this.max(Comparator.naturalOrder());
	}

	public Map<K, BigDecimal> min() {
		return this.min(Comparator.naturalOrder());
	}

	public Table maxTable() {
		return Table.of(this.max(Comparator.naturalOrder()));
	}

	public Table minTable() {
		return Table.of(this.min(Comparator.naturalOrder()));
	}
}
