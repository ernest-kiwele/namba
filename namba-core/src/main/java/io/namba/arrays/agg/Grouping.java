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

package io.namba.arrays.agg;

/**
 * 
 * @author Ernest Kiwele
 */
public interface Grouping {

	int groupCount();

	default int getGroupCount() {
		return this.groupCount();
	}

	Object first(Object key);

	default Object getFirst(Object key) {
		return this.first(key);
	}

	Object last(Object key);

	default Object getLast(Object key) {
		return this.last(key);
	}

	int firstLoc(Object key);

	default int getFirstLoc(Object key) {
		return this.firstLoc(key);
	}

	int lastLoc(Object key);

	default int getLastLoc(Object key) {
		return this.lastLoc(key);
	}

	int depth();

	default int getDepth() {
		return this.depth();
	}

	int nthLoc(Object key, int n);

	int nthLastLoc(Object key, int n);

	Object nthLast(Object key, int n);

	Object nth(Object key, int n);

}
