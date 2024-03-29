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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.Period;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;

import io.namba.arrays.data.tuple.Two;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class DateTimeArray extends DataList<LocalDateTime> {

	private static final DateTimeFormatter DAY_NAME_FORMATTER = DateTimeFormatter.ofPattern("EEEE");
	private static final DateTimeFormatter SHORT_DAY_NAME_FORMATTER = DateTimeFormatter.ofPattern("EEEE");
	private static final Map<Integer, DateTimeFormatter> FORMATTERS = Map.of(4, DateTimeFormatter.ofPattern("yyyy"), 6,
			DateTimeFormatter.ofPattern("yyyyMM"), 8, DateTimeFormatter.ofPattern("yyyyMMdd"), 10,
			DateTimeFormatter.ofPattern("yyyyMMddHH"), 12, DateTimeFormatter.ofPattern("yyyyMMddHHmm"), 14,
			DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

	private DateTimeArray(List<LocalDateTime> is) {
		super(DataType.DATETIME, is);
	}

	private DateTimeArray(DataList<LocalDateTime> is) {
		super(DataType.DATETIME, is.value);
	}

	public static DateTimeArray of(List<LocalDateTime> dt) {
		return new DateTimeArray(dt);
	}

	public static DateTimeArray of(LocalDateTime... dt) {
		return new DateTimeArray(Arrays.asList(dt));
	}

	public static DateTimeArray daysBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.DAYS);
	}

	public static DateTimeArray hoursBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.HOURS);
	}

	public static DateTimeArray minutesBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.MINUTES);
	}

	public static DateTimeArray secondsBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.SECONDS);
	}

	public static DateTimeArray millisecondsBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.MILLIS);
	}

	public static DateTimeArray weeksBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.WEEKS);
	}

	public static DateTimeArray monthsBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.MONTHS);
	}

	public static DateTimeArray yearsBetween(LocalDateTime from, LocalDateTime to) {
		return between(from, to, ChronoUnit.YEARS);
	}

	public static DateTimeArray between(LocalDateTime from, LocalDateTime to, ChronoUnit unit) {
		List<LocalDateTime> dt = new ArrayList<>();
		LocalDateTime start = Objects.requireNonNull(from, "from date is null");
		LocalDateTime end = Objects.requireNonNull(to, "to date is null");

		if (start.isBefore(end)) {
			while (start.isBefore(end)) {
				dt.add(start);
				start = start.plus(1, unit);
			}
		} else {
			while (start.isAfter(end)) {
				dt.add(start);
				start = start.minus(1, unit);
			}
		}

		return new DateTimeArray(dt);
	}

	public static DateTimeArray daysSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.DAYS);
	}

	public static DateTimeArray hoursSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.HOURS);
	}

	public static DateTimeArray minutesSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.MINUTES);
	}

	public static DateTimeArray secondsSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.SECONDS);
	}

	public static DateTimeArray millisecondsSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.MILLIS);
	}

	public static DateTimeArray monthsSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.MONTHS);
	}

	public static DateTimeArray weeksSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.WEEKS);
	}

	public static DateTimeArray yearsSeries(LocalDateTime from, int count, long interval) {
		return series(from, count, interval, ChronoUnit.YEARS);
	}

	public static DateTimeArray series(LocalDateTime from, int count, long interval, ChronoUnit unit) {
		LocalDateTime start = Objects.requireNonNull(from, "from date may not be null");
		if (count < 1) {
			throw new IllegalArgumentException("count must be greater than 0");
		}
		if (interval == 0) {
			throw new IllegalArgumentException("step may not be 0");
		}

		List<LocalDateTime> dt = new ArrayList<>();
		for (int i = 0; i < count; i++) {
			dt.add(start);
			start = start.plus(interval, unit);
		}

		return new DateTimeArray(dt);
	}

	public static DateTimeArray linearFit(LocalDateTime from, LocalDateTime to, long ticks) {
		List<LocalDateTime> dt = new ArrayList<>();
		LocalDateTime start = Objects.requireNonNull(from, "from date is null");
		LocalDateTime end = Objects.requireNonNull(to, "to date is null");
		if (ticks < 1) {
			throw new IllegalArgumentException("ticks must be >= 1");
		}

		long nanos = Math.abs((long) (ChronoUnit.NANOS.between(from, to) / (double) ticks));

		if (start.isBefore(end)) {
			while (start.isBefore(end)) {
				dt.add(start);
				start = start.plusNanos(nanos);
			}
		} else {
			while (start.isAfter(end)) {
				dt.add(start);
				start = start.minusNanos(nanos);
			}
		}

		return new DateTimeArray(dt);
	}

	public static DateTimeArray parse(StringList dates, String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return new DateTimeArray(
				dates.stream().map(s -> LocalDateTime.parse(s, formatter)).collect(Collectors.toList()));
	}

	public DataList<LocalDate> date() {
		List<LocalDate> dates = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			dates.add(null == ldt ? null : ldt.toLocalDate());
		}

		return new DataList<>(DataType.DATE, dates);
	}

	public DataList<LocalTime> time() {
		List<LocalTime> dates = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			dates.add(null == ldt ? null : ldt.toLocalTime());
		}

		return new DataList<>(DataType.TIME, dates);
	}

	public IntList year() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getYear).toArray());
	}

	public IntList month() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getMonthValue).toArray());
	}

	public IntList day() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getDayOfMonth).toArray());
	}

	public StringList dayName() {
		return StringList.of(this.stream().map(dt -> null == dt ? null : dt.format(DAY_NAME_FORMATTER))
				.collect(Collectors.toList()));
	}

	public StringList dayName(String langTag) {
		return StringList.of(this.stream()
				.map(dt -> null == dt ? null
						: dt.format(DateTimeFormatter.ofPattern("EEEE", Locale.forLanguageTag(langTag))))
				.collect(Collectors.toList()));
	}

	public StringList shortDayName() {
		return StringList.of(this.stream().map(dt -> null == dt ? null : dt.format(SHORT_DAY_NAME_FORMATTER))
				.collect(Collectors.toList()));
	}

	public StringList shortDayName(String langTag) {
		return StringList.of(this.stream().map(
				dt -> null == dt ? null : dt.format(DateTimeFormatter.ofPattern("E", Locale.forLanguageTag(langTag))))
				.collect(Collectors.toList()));
	}

	public IntList hour() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getHour).toArray());
	}

	public IntList minute() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getMinute).toArray());
	}

	public IntList second() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getSecond).toArray());
	}

	public IntList nanosecond() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getNano).toArray());
	}

	public IntList dayOfWeek() {
		return IntList.of(this.value.stream().mapToInt(d -> d.getDayOfWeek().getValue()).toArray());
	}

	public DataList<DayOfWeek> dayOfWeekName() {
		return new DataList<>(DataType.OBJECT,
				this.value.stream().map(LocalDateTime::getDayOfWeek).collect(Collectors.toList()));
	}

	public IntList weekDay() {
		return this.dayOfWeek();
	}

	public IntList dayOfYear() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getDayOfYear).toArray());
	}

	public IntList quarter() {
		return IntList.of(this.value.stream().mapToInt(LocalDateTime::getMonthValue).map(i -> 1 + i / 4).toArray());
	}

	public Mask isLeapYear() {
		boolean[] a = new boolean[this.value.size()];

		for (int i = 0; i < this.value.size(); i++) {
			if (null == this.value.get(i))
				continue;

			a[i] = Year.isLeap(this.value.get(i).getYear());
		}

		return Mask.of(a);
	}

	// TODO: Implement
	// public DateTimeArray ceilTo(String precision) {
	// return this.ceilTo(
	// ChronoUnit.valueOf(Objects.requireNonNull(precision, "precision cannot be
	// null").toUpperCase()));
	// }
	// public DateTimeArray ceilTo(ChronoUnit precision) {
	// }

	public DateTimeArray truncateTo(String precision) {
		return this.truncateTo(
				ChronoUnit.valueOf(Objects.requireNonNull(precision, "precision cannot be null").toUpperCase()));
	}

	public DateTimeArray truncateTo(ChronoUnit precision) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.truncatedTo(precision));
		}

		return new DateTimeArray(res);
	}

	public Mask slice(LocalDateTime dateTime, ChronoUnit precision) {
		LocalDateTime comp = Objects.requireNonNull(dateTime, "date may not be blank").truncatedTo(precision);

		return Mask
				.of(this.value.stream().map(dt -> dt == null ? Boolean.FALSE : comp.equals(dt.truncatedTo(precision)))
						.toArray(i -> new Boolean[i]));
	}

	public Mask slice(String dt) {
		String date = dt.replaceAll("[^0-9]", "");
		DateTimeFormatter formatter = FORMATTERS.get(date.length());
		if (null == formatter) {
			throw new IllegalArgumentException("Unsupported slicing pattern: '" + dt + "'");
		}

		return Mask.of(this.value.stream().map(ldt -> ldt == null ? Boolean.FALSE : date.equals(ldt.format(formatter)))
				.toArray(i -> new Boolean[i]));
	}

	public Mask slice(LocalDateTime from, LocalDateTime to) {
		Objects.requireNonNull(from);
		Objects.requireNonNull(to);

		return Mask.of(this.value.stream()
				.map(ldt -> ldt == null ? Boolean.FALSE
						: (from.isBefore(ldt) || from.equals(ldt)) && (to.isAfter(ldt) || to.equals(ldt)))
				.toArray(i -> new Boolean[i]));
	}

	public Mask slice(LocalDate dt) {
		Objects.requireNonNull(dt);

		return Mask.of(this.value.stream().map(ldt -> ldt == null ? Boolean.FALSE : (dt.equals(ldt.toLocalDate())))
				.toArray(i -> new Boolean[i]));
	}

	public Mask slice(LocalDate dt, ChronoUnit precision) {
		return this.slice(LocalDateTime.of(dt, LocalTime.MIDNIGHT), precision);
	}

	public DateTimeArray plus(int days) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.plusDays(days));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray plus(IntList days) {
		if (this.size() != days.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}

		List<LocalDateTime> res = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			res.add(null == this.value.get(i) ? null : this.value.get(i).plusDays(days.getAt(i)));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray plus(Duration d) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.plus(d));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray plus(DataList<Duration> d) {
		if (this.size() != d.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}

		List<LocalDateTime> res = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			res.add(null == this.value.get(i) ? null : this.value.get(i).plus(d.getAt(i)));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray plus(Period p) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.plus(p));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray minus(int days) {
		return this.plus(-days);
	}

	public DateTimeArray minus(IntList days) {
		if (this.size() != days.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}

		List<LocalDateTime> res = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			res.add(null == this.value.get(i) ? null : this.value.get(i).minusDays(days.getAt(i)));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray minus(Duration d) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.minus(d));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray minus(Period p) {
		List<LocalDateTime> res = new ArrayList<>();

		for (LocalDateTime ldt : this.value) {
			res.add(null == ldt ? null : ldt.minus(p));
		}

		return new DateTimeArray(res);
	}

	public DateTimeArray minus(DataList<Duration> d) {
		if (this.size() != d.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}

		List<LocalDateTime> res = new ArrayList<>();

		for (int i = 0; i < this.size(); i++) {
			res.add(null == this.value.get(i) ? null : this.value.get(i).minus(d.getAt(i)));
		}

		return new DateTimeArray(res);
	}

	public IntList minus(LocalDateTime date) {
		// TODO: use long array for this and remove cast
		return IntList.of(this.value.stream().mapToInt(v -> (int) ChronoUnit.DAYS.between(v, date)).toArray());
	}

	public IntList minus(DateTimeArray date) {
		if (this.size() != date.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}
		// TODO: use long array for this and remove cast
		return IntList.of(IntStream.range(0, this.value.size())
				.map(i -> (int) ChronoUnit.DAYS.between(this.value.get(i), date.value.get(i))).toArray());
	}

	public IntList minus(LocalDateTime date, ChronoUnit unit) {
		// TODO: use long array for this and remove cast
		return IntList.of(this.value.stream().mapToInt(v -> (int) unit.between(v, date)).toArray());
	}

	public IntList minus(DateTimeArray date, ChronoUnit unit) {
		if (this.size() != date.size()) {
			throw new IllegalArgumentException("arrays are of different sizes");
		}
		// TODO: use long array for this and remove cast
		return IntList.of(IntStream.range(0, this.value.size())
				.map(i -> (int) unit.between(this.value.get(i), date.value.get(i))).toArray());
	}

	public IntList minus(LocalDateTime date, String unit) {
		return this.minus(date, ChronoUnit.valueOf(StringUtils.upperCase(unit)));
	}

	public IntList minus(DateTimeArray date, String unit) {
		return this.minus(date, ChronoUnit.valueOf(StringUtils.upperCase(unit)));
	}

	public StringList toIsoDate() {
		return StringList.of(this.map(DateTimeFormatter.ISO_DATE::format).value);
	}

	public StringList toIsoTime() {
		return StringList.of(this.map(DateTimeFormatter.ISO_TIME::format).value);
	}

	public StringList toIsoDateTime() {
		return StringList.of(this.map(DateTimeFormatter.ISO_DATE_TIME::format).value);
	}

	public StringList format() {
		return this.toIsoDateTime();
	}

	public StringList format(String pattern) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
		return StringList.of(this.map(formatter::format).value);
	}

	public IntList daysInMonth() {
		int[] v = new int[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null == ldt ? -1 : ldt.getMonth().length(Year.isLeap(ldt.getYear()));
		}

		return IntList.of(v);
	}

	public Mask isMonthEnd() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getDayOfMonth() == ldt.getMonth().length(Year.isLeap(ldt.getYear()));
		}

		return Mask.of(v);
	}

	public Mask isMonthStart() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getDayOfMonth() == 1;
		}

		return Mask.of(v);
	}

	public Mask isQuarterStart() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getMonth().firstMonthOfQuarter() == ldt.getMonth() && ldt.getDayOfMonth() == 1;
		}

		return Mask.of(v);
	}

	public Mask isQuarterEnd() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getMonth().firstMonthOfQuarter().getValue() + 2 == ldt.getMonth().getValue()
					&& ldt.getDayOfMonth() == ldt.getMonth().length(Year.isLeap(ldt.getYear()));
		}

		return Mask.of(v);
	}

	public Mask isYearStart() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getMonth() == Month.JANUARY && 1 == ldt.getDayOfMonth();
		}

		return Mask.of(v);
	}

	public Mask isYearEnd() {
		boolean[] v = new boolean[size()];

		for (int i = 0; i < v.length; i++) {
			LocalDateTime ldt = this.getAt(i);
			v[i] = null != ldt && ldt.getMonth() == Month.DECEMBER && 31 == ldt.getDayOfMonth();
		}

		return Mask.of(v);
	}

	// Agg
	public LocalDateTime min() {
		return this.stream().filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null);
	}

	public LocalDateTime max() {
		return this.stream().filter(Objects::nonNull).max(Comparator.naturalOrder()).orElse(null);
	}

	//

	public Map<LocalDateTime, int[]> groupBy(ChronoUnit frequency, LocalDateTime offset) {
		return this.groupBy(frequency.getDuration(), offset);
	}

	public Map<LocalDateTime, int[]> groupBy(ChronoUnit frequency) {
		return this.groupBy(frequency, this.min());
	}

	public Map<LocalDateTime, int[]> groupBy(Duration duration, LocalDateTime offset) {
		return IntStream.range(0, this.size()).mapToObj(i -> Two.of(this.groupKey(offset, this.getAt(i), duration), i))
				.collect(Collectors.groupingBy(Two::a, Collectors.mapping(Two::b, Collectors
						.collectingAndThen(Collectors.toList(), l -> l.stream().mapToInt(v -> v).toArray()))));
	}

	public Map<LocalDateTime, int[]> groupBy(Duration duration) {
		return this.groupBy(duration, this.min());
	}

	public Map<LocalDateTime, List<LocalDateTime>> groupValuesBy(ChronoUnit frequency, LocalDateTime offset) {
		return stream().collect(Collectors.groupingBy(e -> this.groupKey(offset, e, frequency), Collectors.toList()));
	}

	public Map<LocalDateTime, List<LocalDateTime>> groupValuesBy(ChronoUnit frequency) {
		return this.groupValuesBy(frequency, this.min());
	}

	public Map<LocalDateTime, List<LocalDateTime>> groupValuesBy(Duration duration, LocalDateTime offset) {
		return stream().collect(Collectors.groupingBy(e -> this.groupKey(offset, e, duration), Collectors.toList()));
	}

	public Map<LocalDateTime, List<LocalDateTime>> groupValuesBy(Duration duration) {
		return this.groupValuesBy(duration, this.min());
	}

	private LocalDateTime groupKey(LocalDateTime base, LocalDateTime dt, Duration d) {
		return dt.minus(ChronoUnit.NANOS.between(base, dt) % d.toNanos(), ChronoUnit.NANOS);
	}

	private LocalDateTime groupKey(LocalDateTime base, LocalDateTime dt, ChronoUnit unit) {
		// this is based on the flooring of the diff returned by unit.between.
		return base.plus(unit.between(base, dt), unit);
	}

	/**
	 * difference between previous row and current row.
	 * 
	 * @param unit
	 * @return
	 */
	public LongList diff(ChronoUnit unit) {
		DataList<LocalDateTime> oneOff = this.shift();
		DataList<Long> lst = this.zipTo(oneOff, unit::between);
		return lst.asLong();
	}

	public LongList diff(String unit) {
		return this.diff(ChronoUnit.valueOf(unit));
	}
	/*
	 * Select final periods of time series data based on a date offset.
	 * 
	 * When having a DataFrame with dates as index, this function can select the
	 * last few rows based on a date offset.
	 */
	// TODO: evaluate and/or implement
	// public DateTimeArray last(LocalDateTime offset) {
	//
	// }
	// public DateTimeArray shift(ChronoUnit unit, long value) {
	//
	// }
	// public DateTimeArray shift(ChronoUnit unit) {
	// return this.shift(unit, 1L);
	// }

	public static void main(String[] args) {
		DateTimeArray dta = DateTimeArray.linearFit(LocalDateTime.of(LocalDate.of(2016, 10, 12), LocalTime.MIDNIGHT),
				LocalDateTime.of(LocalDate.now().minusDays(1), LocalTime.MIDNIGHT), 8).plus(Duration.ofDays(5));
		System.out.println(dta);
		System.out.println();
		System.out.println(
				dta.groupValuesBy(ChronoUnit.YEARS, LocalDateTime.of(LocalDate.of(2016, 1, 1), LocalTime.MIDNIGHT)));
	}
}
