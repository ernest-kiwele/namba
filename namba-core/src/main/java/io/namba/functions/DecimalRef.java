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
import io.namba.arrays.Mask;

/**
 * 
 * @author Ernest Kiwele
 *
 */
public class DecimalRef {

	private final DecimalPredicate filter;
	private final DecimalList refPointer;

	private DecimalRef(DecimalPredicate filter, DecimalList pointer) {
		this.filter = filter;
		this.refPointer = pointer;
	}

	private DecimalRef(DecimalPredicate filter) {
		this(filter, null);
	}

	public static DecimalRef full() {
		return new DecimalRef(null);
	}

	public static DecimalRef where(DecimalPredicate filter, DecimalList refPointer) {
		return new DecimalRef(filter, refPointer);
	}

	public static DecimalRef where(DecimalPredicate filter) {
		return new DecimalRef(filter, null);
	}

	public DecimalRef withList(DecimalList list) {
		return where(this.filter, list);
	}

	public DecimalRef withPredicate(DecimalPredicate filter) {
		return where(filter, this.refPointer);
	}

	public Mask resolveMask(DecimalList list) {
		if (null == this.filter) {
			return Mask.trues(list.size());
		} else {
			return this.filter.test(list);
		}
	}

	public DecimalList resolveList(DecimalList list) {
		return list.getAt(null != this.filter ? this.filter.test(list) : Mask.trues(list.size()));
	}

	public Mask mask() {
		return this.resolveMask(this.refPointer);
	}

	public DecimalList list() {
		return this.resolveList(this.refPointer);
	}

	@FunctionalInterface
	public interface DecimalPredicate {
		Mask test(DecimalList v);

		default DecimalPredicate and(DecimalPredicate p) {
			return il -> this.test(il).and(p.test(il));
		}

		default DecimalPredicate or(DecimalPredicate p) {
			return il -> this.test(il).or(p.test(il));
		}

		default DecimalPredicate xor(DecimalPredicate p) {
			return il -> this.test(il).xor(p.test(il));
		}
	}

	public DecimalRef gt(BigDecimal v) {
		return this.and(i -> i.gt(v));
	}

	public DecimalRef gt(DecimalList v) {
		return this.and(i -> i.gt(v));
	}

	public DecimalRef lt(BigDecimal v) {
		return this.and(i -> i.lt(v));
	}

	public DecimalRef lt(DecimalList v) {
		return this.and(i -> i.lt(v));
	}

	public DecimalRef ge(BigDecimal v) {
		return this.and(i -> i.ge(v));
	}

	public DecimalRef ge(DecimalList v) {
		return this.and(i -> i.ge(v));
	}

	public DecimalRef le(BigDecimal v) {
		return this.and(i -> i.le(v));
	}

	public DecimalRef le(DecimalList v) {
		return this.and(i -> i.le(v));
	}

	public DecimalRef zero() {
		return this.and(i -> i.eq(BigDecimal.ZERO));
	}

	public DecimalRef nonZero() {
		return this.and(i -> i.ne(BigDecimal.ZERO));
	}

	public DecimalRef and(DecimalPredicate p) {
		return where(this.filter.and(p), this.refPointer);
	}

	public DecimalRef or(DecimalPredicate p) {
		return where(this.filter.and(p), this.refPointer);
	}

	public DecimalRef xor(DecimalPredicate p) {
		return where(this.filter.xor(p), this.refPointer);
	}
}
