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

import io.namba.arrays.IntList;
import io.namba.arrays.Mask;

/**
 * 
 * @author Ernest Kiwele
 */
public class IntRef {

	private final IntListPredicate filter;
	private final IntList refPointer;

	private IntRef(IntListPredicate filter, IntList pointer) {
		this.filter = filter;
		this.refPointer = pointer;
	}

	private IntRef(IntListPredicate filter) {
		this(filter, null);
	}

	public static IntRef full() {
		return new IntRef(null);
	}

	public static IntRef where(IntListPredicate filter, IntList refPointer) {
		return new IntRef(filter, refPointer);
	}

	public static IntRef where(IntListPredicate filter) {
		return new IntRef(filter, null);
	}
	
	public IntRef withList(IntList list) {
		return where(this.filter, list);
	}
	public IntRef withPredicate(IntListPredicate filter) {
		return where(filter, this.refPointer);
	}

	public Mask resolveMask(IntList list) {
		if (null == this.filter) {
			return Mask.trues(list.size());
		} else {
			return this.filter.test(list);
		}
	}

	public IntList resolveList(IntList list) {
		return list.getAt(null != this.filter ? this.filter.test(list) : Mask.trues(list.size()));
	}

	public Mask mask() {
		return this.resolveMask(this.refPointer);
	}

	public IntList list() {
		return this.resolveList(this.refPointer);
	}

	@FunctionalInterface
	public interface IntListPredicate {
		Mask test(IntList v);

		default IntListPredicate and(IntListPredicate p) {
			return il -> this.test(il).and(p.test(il));
		}

		default IntListPredicate or(IntListPredicate p) {
			return il -> this.test(il).or(p.test(il));
		}

		default IntListPredicate xor(IntListPredicate p) {
			return il -> this.test(il).xor(p.test(il));
		}
	}

	public IntRef gt(int v) {
		return this.and(i -> i.gt(v));
	}

	public IntRef gt(IntList v) {
		return this.and(i -> i.gt(v));
	}

	public IntRef lt(int v) {
		return this.and(i -> i.lt(v));
	}

	public IntRef lt(IntList v) {
		return this.and(i -> i.lt(v));
	}

	public IntRef ge(int v) {
		return this.and(i -> i.ge(v));
	}

	public IntRef ge(IntList v) {
		return this.and(i -> i.ge(v));
	}

	public IntRef le(int v) {
		return this.and(i -> i.le(v));
	}

	public IntRef le(IntList v) {
		return this.and(i -> i.le(v));
	}

	public IntRef zero() {
		return this.and(i -> i.eq(0));
	}

	public IntRef nonZero() {
		return this.and(i -> i.ne(0));
	}

	public IntRef even() {
		return this.and(i -> i.mod(2).eq(0));
	}

	public IntRef odd() {
		return this.and(i -> i.mod(2).eq(1));
	}

	public IntRef and(IntListPredicate p) {
		return where(this.filter.and(p), this.refPointer);
	}

	public IntRef or(IntListPredicate p) {
		return where(this.filter.and(p), this.refPointer);
	}

	public IntRef xor(IntListPredicate p) {
		return where(this.filter.xor(p), this.refPointer);
	}
}
