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

package io.namba.functions;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import io.namba.arrays.DecimalList;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class NambaMath {

	private NambaMath() {
	}

	public static BigDecimal max(BigDecimal bd1, BigDecimal bd2) {
		if (null == bd1)
			return bd2;
		else if (null == bd2)
			return bd1;

		return bd1.compareTo(bd2) > 0 ? bd1 : bd2;
	}

	public static BigDecimal min(BigDecimal bd1, BigDecimal bd2) {
		if (null == bd1)
			return bd2;
		else if (null == bd2)
			return bd1;

		return bd1.compareTo(bd2) < 0 ? bd1 : bd2;
	}

	public static BigDecimal firstNonNull(BigDecimal bd1, BigDecimal bd2) {
		return bd1 != null ? bd1 : bd2;
	}

	public static BigDecimal mean(BigDecimal bd1, BigDecimal bd2) {
		return bd1.add(bd2).divide(BigDecimal.valueOf(2), DecimalList.DEFAULT_MATH_CONTEXT);
	}

	// TODO: cache these MathContext objects?
	public static BigDecimal truncate(BigDecimal bd, int to) {
		if (to >= 0) {
			return bd.setScale(to, RoundingMode.HALF_UP);
		} else {
			BigDecimal tmp = bd.setScale(0, RoundingMode.HALF_UP);
			int precision = tmp.precision();

			return to + precision <= 0 ? BigDecimal.ZERO : tmp.round(new MathContext(precision + to));// ? 0 : precision
		}
	}

	public static BigDecimal mean(Collection<BigDecimal> coll) {
		if (null == coll || coll.isEmpty())
			return null;

		BigDecimal a = BigDecimal.ZERO;
		for (BigDecimal bd : coll) {
			if (bd != null)
				a = a.add(bd);
		}

		return a;
	}

	public static BigDecimal median(Collection<BigDecimal> coll) {
		if (null == coll || coll.isEmpty())
			return null;

		List<BigDecimal> l = new ArrayList<>();
		l.sort(Comparator.naturalOrder());

		if (l.size() % 2 == 1) {
			return l.get(1 + (l.size() / 2));
		}

		if (l.size() == 1) {
			return l.get(0);
		}

		int mid = l.size() / 2;
		return mean(l.get(mid), l.get(mid + 1));
	}

	public static BigDecimal populationVar(Collection<BigDecimal> coll, MathContext mathContext) {
		if (null == coll || coll.isEmpty())
			return null;

		BigDecimal mean = mean(coll);
		return coll.stream().map(i -> i.subtract(mean, mathContext).pow(2))
				.reduce(BigDecimal.ZERO, (a, b) -> a.add(b, mathContext)).divide(BigDecimal.valueOf(coll.size()));
	}

	public static BigDecimal sampleVar(Collection<BigDecimal> coll, MathContext mathContext) {
		if (null == coll || coll.isEmpty())
			return null;

		BigDecimal mean = mean(coll);
		return coll.stream().map(i -> i.subtract(mean).pow(2)).reduce(BigDecimal.ZERO, (a, b) -> a.add(b, mathContext))
				.divide(BigDecimal.valueOf(coll.size() - 1l), mathContext);
	}

	public static BigDecimal populationStd(Collection<BigDecimal> coll, MathContext mathContext) {
		if (null == coll || coll.isEmpty())
			return null;

		return populationVar(coll, mathContext).sqrt(mathContext);
	}

	public static BigDecimal sampleStd(Collection<BigDecimal> coll, MathContext mathContext) {
		if (null == coll || coll.isEmpty())
			return null;

		return sampleVar(coll, mathContext).sqrt(mathContext);
	}
}
