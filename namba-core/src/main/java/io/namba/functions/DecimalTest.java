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
import java.util.function.Function;

import io.namba.arrays.DecimalList;
import io.namba.arrays.Mask;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public enum DecimalTest {

	IS_NAN(list -> {
		boolean[] b = new boolean[list.size()];
		for (int i = 0; i < b.length; i++) {
			b[i] = null == list.getAt(i);
		}
		return Mask.of(b);
	}),

	ZERO(list -> {
		boolean[] b = new boolean[list.size()];
		for (int i = 0; i < b.length; i++) {
			BigDecimal bd = list.getAt(i);
			b[i] = null != bd && BigDecimal.ZERO.equals(bd);
		}
		return Mask.of(b);
	}),

	NON_ZERO(list -> {
		boolean[] b = new boolean[list.size()];
		for (int i = 0; i < b.length; i++) {
			BigDecimal bd = list.getAt(i);
			b[i] = null != bd && !BigDecimal.ZERO.equals(bd);
		}
		return Mask.of(b);
	})

	// TODO: Add values that support or don't support NAs
	;

	private final Function<DecimalList, Mask> test;

	private DecimalTest(Function<DecimalList, Mask> t) {
		this.test = t;
	}

	public Mask test(DecimalList list) {
		return this.test.apply(list);
	}

	public boolean any(DecimalList list) {
		return this.test(list).any();
	}

	public boolean all(DecimalList list) {
		return this.test(list).all();
	}
}
