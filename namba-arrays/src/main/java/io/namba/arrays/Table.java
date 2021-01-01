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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import io.namba.Namba;
import io.namba.arrays.range.IntRange;

/**
 * 
 * @author Ernest Kiwele
 */
public class Table implements NambaList {

	protected final int size;
	protected final Index index;
	protected final List<NambaList> columns;
	protected final Map<String, Integer> names;

	public Table(List<NambaList> columns, Index index) {
		this.validateColumnLengths(columns);

		this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
		this.index = index;
		this.size = columns.get(0).size();

		this.names = IntStream.range(0, columns.size()).mapToObj(i -> Pair.of(i, columns.get(i).getName()))
				.map(c -> Pair.of(StringUtils.isBlank(c.getRight()) ? String.valueOf(c.getLeft()) : c.getRight(),
						c.getLeft()))
				.collect(Collectors.toUnmodifiableMap(Pair::getLeft, Pair::getRight));
	}

	public static Table of(List<NambaList> columns, Index index) {
		return new Table(columns, index);
	}

	public static Table of(Index index, NambaList... columns) {
		return new Table(Arrays.asList(columns), index);
	}

	private void validateColumnLengths(List<NambaList> cols) {
		if (Objects.requireNonNull(cols, "column list is null").isEmpty()) {
			throw new IllegalArgumentException("Column list is is empty");
		}

		if (1 < IntStream.range(0, cols.size())
				.mapToObj(i -> Pair.of(i, Objects.requireNonNull(cols.get(i), "found null column at index " + i)))
				.mapToInt(p -> p.getRight().size()).distinct().count()) {
			throw new IllegalArgumentException("different list sizes detected in columns");
		}
	}

	@Override
	public DataType dataType() {
		return null;
	}

	@Override
	public Index index() {
		return index;
	}

	@Override
	public int size() {
		return this.size;
	}

	public Table getAt(int[] loc) {
		return Table.of(this.columns.stream().map(col -> col.getAt(loc)).collect(Collectors.toList()), null);
	}

	public Table getAt(int start, int end) {
		return Table.of(this.columns.stream().map(col -> col.getAt(IntRange.of(start, end).stream().toArray()))
				.collect(Collectors.toList()), null);
	}

	public Table head(int size) {
		return this.getAt(0, size);
	}

	public Table tail(int size) {
		return this.getAt(this.size - size, this.size);
	}

	public Table head() {
		return this.head(SUMMARY_SIZE);
	}

	public Table tail() {
		return this.tail(SUMMARY_SIZE);
	}

	// TODO: Implement these lookups based on index values.
	// public Object at(Object row, String column) {
	//
	// }
	//
	// public Object iat(int row, int column) {
	//
	// }

	@Override
	public NambaList repeat(int n) {
		return Table.of(this.columns.stream().map(c -> c.repeat(n)).collect(Collectors.toList()), null);
	}

	@Override
	public String toString() {
		if (SUMMARY_SIZE * 2 < this.size)
			return head().string().toString() + "\n[...]\n" + tail().string().toString();
		else
			return string().toString();
	}

	@Override
	public StringList string() {
		List<StringList> strings = Stream
				.of(Stream.<NambaList>of(Namba.instance().data.ints.range(this.size)), this.columns.stream())
				.flatMap(Function.identity()).map(NambaList::string).map(StringList::leftPadToMaxLength)
				.collect(Collectors.toList());

		return StringList.of(IntStream.range(0, this.size())
				.mapToObj(i -> strings.stream().map(s -> s.getAt(i)).collect(Collectors.joining(" | ", "| ", " |")))
				.collect(Collectors.joining("\n")));
	}

	public NambaList getAt(String name) {
		return this.columns.get(this.names.get(name));
	}

	public static void main(String[] args) {
		Namba nb = Namba.instance();
		System.out.println(Table.of(List.of(nb.data.doubles.geomSpace(12, 1000, 4)), null));
	}
}
