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
import java.util.function.BinaryOperator;

import io.namba.arrays.DecimalList;

/**
 * 
 * @author Ernest Kiwele
 */
public class DecimalFunctions {

	private static final DecimalFunctions instance = new DecimalFunctions();

	private DecimalFunctions() {
	}

	public static DecimalFunctions instance() {
		return instance;
	}

	public BinaryOperator<BigDecimal> wrapWithNullCheck(BinaryOperator<BigDecimal> op) {
		return (a, b) -> a == null || b == null ? null : op.apply(a, b);
	}

	public final BinaryOperator<BigDecimal> plus = this
			.wrapWithNullCheck((a, b) -> a.add(b, DecimalList.DEFAULT_MATH_CONTEXT));
	public final BinaryOperator<BigDecimal> add = this.plus;

	public final BinaryOperator<BigDecimal> times = this
			.wrapWithNullCheck((a, b) -> a.multiply(b, DecimalList.DEFAULT_MATH_CONTEXT));
	public final BinaryOperator<BigDecimal> multiply = this.times;

	public final BinaryOperator<BigDecimal> minus = this
			.wrapWithNullCheck((a, b) -> a.subtract(b, DecimalList.DEFAULT_MATH_CONTEXT));
	public final BinaryOperator<BigDecimal> subtract = this.minus;

	public final BinaryOperator<BigDecimal> div = this
			.wrapWithNullCheck((a, b) -> a.divide(b, DecimalList.DEFAULT_MATH_CONTEXT));
	public final BinaryOperator<BigDecimal> divide = this.minus;
}
