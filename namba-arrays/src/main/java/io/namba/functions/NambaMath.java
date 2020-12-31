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
}
