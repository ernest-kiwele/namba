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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalDouble;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class IntMath {

	private IntMath() {
	}

	public static int max(int bd1, int bd2) {
		return Math.max(bd1, bd2);
	}

	public static int min(int bd1, int bd2) {
		return Math.min(bd1, bd2);
	}

	public static double mean(int bd1, int bd2) {
		return (bd1 + bd2) / 2.0;
	}

	// TODO: cache these MathContext objects?
	public static int truncate(int bd, int to) {
		return DecimalMath.truncate(BigDecimal.valueOf(bd), to).intValue();
	}

	public static OptionalDouble mean(Collection<Integer> coll) {
		if (null == coll || coll.isEmpty())
			return OptionalDouble.empty();

		double a = 0;

		for (int bd : coll) {
			a += bd;
		}

		return OptionalDouble.of(a / coll.size());
	}

	public static double median(Collection<Integer> coll) {
		if (null == coll || coll.isEmpty())
			throw new IllegalArgumentException("collection is null or empty");

		List<Integer> l = new ArrayList<>(coll);
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
}
