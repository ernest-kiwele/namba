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

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.WordUtils;

/**
 * 
 * @author Ernest Kiwele
 */
public class StringList extends DataList<String> {

	public final StringAccessor str = new StringAccessor();

	private StringList(List<String> values) {
		super(DataType.STRING, Objects.requireNonNull(values));
	}

	public static StringList of(String... str) {
		return new StringList(Arrays.asList(str));
	}

	public static StringList of(List<String> str) {
		return new StringList(str);
	}

	public static StringList of(Collection<String> str) {
		return new StringList(new ArrayList<>(str));
	}

	public StringList insertAtHead(List<String> values) {
		List<String> all = new ArrayList<>(values);
		all.addAll(this.value);

		return new StringList(all);
	}

	public StringList prepend(String other) {
		List<String> copy = new ArrayList<>(this.value.size());
		for (String s : this.value) {
			if (null == s) {
				copy.add(other);
			} else {
				copy.add(other + s);
			}
		}
		return new StringList(copy);
	}

	public StringList plus(String other) {
		List<String> copy = new ArrayList<>(this.value.size());
		for (String s : this.value) {
			if (null == s) {
				copy.add(other);
			} else {
				copy.add(s + other);
			}
		}
		return new StringList(copy);
	}

	public StringList append(String other) {
		return this.plus(other);
	}

	public StringList concat(String other) {
		return this.plus(other);
	}

	/*
	 * Pad left and right side of strings in the Series/Index.
	 */
	public StringList center() {
		int maxLength = this.maxLength();

		List<String> v = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			v.add(StringUtils.center(this.getAt(i), maxLength));
		}

		return of(v);
	}

	public StringList minus(String other) {
		List<String> copy = new ArrayList<>(this.value.size());
		for (String s : this.value) {
			if (null == s) {
				copy.add(null);
			} else {
				copy.add(s.replace(other, ""));
			}
		}
		return new StringList(copy);
	}

	public StringList multiply(int times) {
		List<String> copy = new ArrayList<>(this.value.size());
		for (String s : this.value) {
			if (null == s) {
				copy.add(null);
			} else {
				copy.add(StringUtils.repeat(s, times));
			}
		}
		return new StringList(copy);
	}

	@Override
	public StringList repeat(int times) {
		return this.multiply(times);
	}

	public StringList repeat(IntList times) {
		List<String> v = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			String val = this.value.get(i);
			if (null == val)
				v.add(null);
			else
				v.add(StringUtils.repeat(val, times.value[i]));
		}

		return of(v);
	}

	public Mask contains(String str) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null ? false : v.contains(str);
		}

		return Mask.of(b);
	}

	public Mask contains(StringList str) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null ? false : v.contains(str.getAt(i));
		}

		return Mask.of(b);

	}

	public Mask endsWith(String str) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null ? false : v.endsWith(str);
		}

		return Mask.of(b);
	}

	public Mask endsWith(StringList str) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v != null && v.endsWith(str.getAt(i));
		}

		return Mask.of(b);
	}

	public Mask startsWith(String str) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null ? false : v.startsWith(str);
		}

		return Mask.of(b);
	}

	public StringList lower() {
		return StringList.of(this.map(String::toLowerCase).value);
	}

	public StringList upper() {
		return StringList.of(this.map(String::toUpperCase).value);
	}

	public StringList capitalize() {
		return StringList.of(this.map(StringUtils::capitalize).value);
	}

	public StringList capitalizeFully() {
		return StringList.of(this.map(WordUtils::capitalizeFully).value);
	}

	public StringList title() {
		return this.capitalizeFully();
	}

	public StringList swapCase() {
		return StringList.of(this.map(StringUtils::swapCase).value);
	}

	public IntList length() {
		return IntList.of(this.stream().mapToInt(s -> s == null ? -1 : s.length()).toArray());
	}

	public IntList len() {
		return this.length();
	}

	public int maxLength() {
		return this.length().max().orElse(0);
	}

	public IntList count(String search) {
		return IntList.of(this.stream().mapToInt(s -> s == null ? -1 : StringUtils.countMatches(s, search)).toArray());
	}

	public IntList count(StringList search) {
		int[] r = new int[this.value.size()];

		for (int i = 0; i < size(); i++) {
			r[i] = StringUtils.countMatches(this.value.get(i), search.getAt(i));
		}

		return IntList.of(r);
	}

	public StringList extract(String pattern, int group) {
		Pattern compiledPattern = Pattern.compile(pattern);

		List<String> b = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			if (null == this.value.get(i))
				b.add(null);
			else {
				Matcher matcher = compiledPattern.matcher(this.value.get(i));
				if (matcher.find()) {
					if (matcher.groupCount() >= group)
						b.add(matcher.group(group));
					else
						b.add(null);
				} else {
					b.add(null);
				}
			}
		}

		return StringList.of(b);
	}

	// TODO: Implement
	// public StringList extract(StringList pattern, IntList group) {
	//
	// }

	public DataList<List<String>> extractAll(String pattern) {
		return this.extractAll(pattern, 1);
	}
	// TODO: Implement
	// public DataList<List<String>> extractAll(StringList pattern) {
	// }

	public DataList<List<String>> extractAll(String pattern, int group) {
		Pattern compiledPattern = Pattern.compile(pattern);

		List<List<String>> b = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			if (null == this.value.get(i))
				b.add(null);
			else {
				Matcher matcher = compiledPattern.matcher(this.value.get(i));
				List<String> current = new ArrayList<>();
				while (matcher.find()) {
					if (matcher.groupCount() >= group) {
						current.add(matcher.group(group));
					}
				}
				b.add(current);
			}
		}

		return new DataList<>(DataType.OBJECT, b);
	}

	// TODO: Implement
	// public DataList<List<String>> extractAll(StringList pattern, IntList group) {
	// }

	public IntList indexOf(String string) {
		return IntList.of(this.stream().mapToInt(s -> s == null ? Integer.MIN_VALUE : s.indexOf(string)).toArray());
	}

	public IntList find(String string) {
		return this.indexOf(string);
	}

	public IntList indexOf(StringList string) {
		int[] v = new int[this.size()];

		for (int i = 0; i < v.length; i++) {
			String val = this.getAt(i);
			v[i] = null == val ? Integer.MIN_VALUE : val.indexOf(string.getAt(i));
		}

		return IntList.of(v);
	}

	public IntList find(StringList string) {
		return this.indexOf(string);
	}

	public IntList lastIndexOf(String string) {
		return IntList.of(this.stream().mapToInt(s -> s == null ? Integer.MIN_VALUE : s.lastIndexOf(string)).toArray());
	}

	public IntList findLast(String string) {
		return this.lastIndexOf(string);
	}

	public IntList lastIndexOf(StringList string) {
		int[] v = new int[this.size()];

		for (int i = 0; i < v.length; i++) {
			String val = this.getAt(i);
			v[i] = null == val ? Integer.MIN_VALUE : val.lastIndexOf(string.getAt(i));
		}

		return IntList.of(v);

	}

	public IntList findLast(StringList string) {
		return this.lastIndexOf(string);
	}

	public StringList trim() {
		return StringList.of(this.map(StringUtils::trim).value);
	}

	public Mask matches(String pattern) {
		return Mask.of(this.stream().map(s -> null != s && s.matches(pattern)).toArray(i -> new Boolean[i]));
	}

	public Mask matches(StringList pattern) {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < b.length; i++) {
			String text = this.getAt(i);
			String patt = pattern.getAt(i);

			b[i] = null != text && null != patt && text.matches(patt);
		}

		return Mask.of(b);
	}

	public StringList rightPadToMaxLength() {
		return this.rightPad(this.maxLength());
	}

	public StringList rightPad(int length) {
		return StringList.of(this.map(s -> StringUtils.rightPad(s, length)).value);
	}

	public StringList rightPad(int length, String padChar) {
		return StringList.of(this.map(s -> StringUtils.rightPad(s, length, padChar)).value);
	}

	public StringList leftPadToMaxLength() {
		return this.leftPad(this.maxLength());
	}

	public StringList leftPad(int length) {
		return StringList.of(this.map(s -> StringUtils.leftPad(s, length)).value);
	}

	public StringList zfill(int legth) {
		return this.leftPad(legth, "0");
	}

	public StringList zfill() {
		return this.leftPad(this.maxLength(), "0");
	}

	public StringList leftPad(int length, String padChar) {
		return StringList.of(this.map(s -> StringUtils.leftPad(s, length, padChar)).value);
	}

	public StringList replace(String search, String replacement) {
		return StringList.of(this.map(s -> null == s ? null : s.replace(search, replacement)).value);
	}

	public StringList replace(StringList search, StringList replacement) {
		List<String> v = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String t = this.getAt(i);
			String s = search.getAt(i);
			String r = replacement.getAt(i);

			v.add(null == t || null == s || null == r ? null : t.replace(s, r));
		}

		return of(v);
	}

	public StringList replaceMatch(String search, String replacement) {
		return StringList.of(this.map(s -> null == s ? null : s.replaceAll(search, replacement)).value);
	}

	public StringList replaceMatch(StringList search, StringList replacement) {
		List<String> v = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String t = this.getAt(i);
			String s = search.getAt(i);
			String r = replacement.getAt(i);

			v.add(null == t || null == s || null == r ? null : t.replaceAll(s, r));
		}

		return of(v);
	}

	public Mask isEmpty() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null || v.isEmpty();
		}

		return Mask.of(b);
	}

	public Mask isBlank() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v == null || v.trim().isEmpty();
		}

		return Mask.of(b);
	}

	public Mask isNotEmpty() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v != null && !v.isEmpty();
		}

		return Mask.of(b);
	}

	public Mask isNotBlank() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			String v = this.value.get(i);
			b[i] = v != null && !v.trim().isEmpty();
		}

		return Mask.of(b);
	}

	public Mask isAlphaNumeric() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			b[i] = StringUtils.isAlphanumeric(this.value.get(i));
		}

		return Mask.of(b);
	}

	public Mask isAlpha() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			b[i] = StringUtils.isAlpha(this.value.get(i));
		}

		return Mask.of(b);
	}

	public Mask isNumeric() {
		boolean[] b = new boolean[this.size()];

		for (int i = 0; i < this.size(); i++) {
			b[i] = StringUtils.isNumeric(this.value.get(i));
		}

		return Mask.of(b);
	}

	public Mask isLower() {
		boolean[] b = new boolean[size()];

		for (int i = 0; i < b.length; i++) {
			String val = this.getAt(i);
			b[i] = null != val && StringUtils.isAllLowerCase(val);
		}

		return Mask.of(b);
	}

	public Mask isUpper() {
		boolean[] b = new boolean[size()];

		for (int i = 0; i < b.length; i++) {
			String val = this.getAt(i);
			b[i] = null != val && StringUtils.isAllUpperCase(val);
		}

		return Mask.of(b);
	}

	@Override
	public StringList string() {
		return this;
	}

	@Override
	public String toString() {
		return this.value.stream().collect(Collectors.joining("\n"));
	}

	public CategoryList factorize() {
		return CategoryList.of(this);
	}

	public CategoryList asCategory() {
		return this.factorize();
	}

	public StringList toBase64() {
		List<String> b = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String val = this.getAt(i);
			b.add(null == val ? null : Base64.getEncoder().encodeToString(val.getBytes()));
		}

		return of(b);
	}

	public StringList fromBase64() {
		List<String> b = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String val = this.getAt(i);
			b.add(null == val ? null : new String(Base64.getDecoder().decode(val)));
		}

		return of(b);
	}

	public DataList<List<String>> split(String delimiter) {
		List<List<String>> b = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String val = this.getAt(i);
			b.add(null == val ? Collections.emptyList() : Arrays.asList(val.split(delimiter)));
		}

		return new DataList<>(DataType.OBJECT, b);
	}

	public DataList<List<String>> split(StringList delimiter) {
		List<List<String>> b = new ArrayList<>();

		for (int i = 0; i < size(); i++) {
			String val = this.getAt(i);
			String del = delimiter.getAt(i);

			b.add(null == val ? Collections.emptyList() : Arrays.asList(val.split(del)));
		}

		return new DataList<>(DataType.OBJECT, b);
	}

	public class StringAccessor {
		public StringList getAt(int n) {
			List<String> list = new ArrayList<>();
			for (String s : value) {
				if (null == s) {
					list.add(null);
				} else {
					list.add(s.substring(n));
				}
			}
			return new StringList(list);
		}

		public StringList getAt(IntList n) {
			List<String> list = new ArrayList<>();
			for (int i = 0; i < size(); i++) {
				String s = StringList.this.getAt(i);
				if (null == s) {
					list.add(null);
				} else {
					list.add(s.substring(n.getAt(i)));
				}
			}
			return new StringList(list);
		}

		public StringList getAt(int start, int length) {
			List<String> list = new ArrayList<>();
			for (String s : value) {
				if (null == s) {
					list.add(null);
				} else {
					list.add(s.substring(start, start + length));
				}
			}
			return new StringList(list);
		}

		public StringList getAt(IntList start, IntList length) {
			List<String> list = new ArrayList<>();
			for (int i = 0; i < size(); i++) {
				String s = StringList.this.getAt(i);
				if (null == s) {
					list.add(null);
				} else {
					int st = start.getAt(i);
					list.add(s.substring(st, st + length.value[i]));
				}
			}
			return new StringList(list);
		}
	}

	public static void main(String[] args) {
		System.out.println(DateTimeArray
				.linearFit(LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT),
						LocalDateTime.of(LocalDate.now().minusDays(10), LocalTime.MIDNIGHT), 4)
				.plus(Duration.ofDays(5)).format("yyyy-MM-dd hh:mm:ssa").groupBy(s -> s.substring(0, 4)).countTable());
	}
}
