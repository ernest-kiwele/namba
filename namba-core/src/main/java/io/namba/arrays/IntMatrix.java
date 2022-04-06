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

import io.namba.arrays.data.IntData;
import io.namba.arrays.data.IntPair;
import io.namba.arrays.data.tuple.TwoInts;
import io.namba.arrays.range.Slice;

/**
 * 
 * @author Ernest Kiwele
 */
public class IntMatrix extends IntList {

	private final int width;
	private final int height;

	protected IntMatrix(int[] val, int width) {
		super(val);

		if (0 != val.length % width) {
			throw new IllegalArgumentException("array's length must be a multiple of width");
		}
		this.width = width;
		this.height = val.length / width;
	}

	public int getAt(TwoInts coordinates) {
		return super.getAt(this.width * coordinates.a() + coordinates.b());
	}

	public IntList getAt(Slice rowSlice) {
		return this.getAt(rowSlice.apply(this.size()));
	}

	public IntMatrix getAt(Slice rowSlice, Slice colSlice) {
		int[] down = rowSlice.apply(this.height);
		int[] across = colSlice.apply(this.width);

		int[] newMatrix = new int[down.length * across.length];

		for (int i = 0; i < down.length; i++) {
			for (int e = 0; e < across.length; e++) {
				int a = this.width * down[i];
				int b = across[e];

				newMatrix[i * across.length + e] = this.value[a + b];
			}
		}

		return new IntMatrix(newMatrix, across.length);
	}

	public int[][] to2D() {
		int[][] res = new int[this.height][this.width];

		for (int i = 0; i < this.height; i++) {
			int[] line = new int[this.width];
			System.arraycopy(this.value, i * this.width, line, 0, this.width);
			res[i] = line;
		}

		return res;
	}

	@Override
	public String toString() {
		StringList sl = this.string().leftPadToMaxLength().prepend(" ");
		StringBuilder text = new StringBuilder();

		for (int i = 0; i < this.height; i++) {
			for (int e = 0; e < this.width; e++) {
				text.append(sl.getAt(i * this.width + e));
			}
			text.append("\n");
		}

		return text.toString();
	}

	public IntMatrix reshape(int newWidth) {
		return new IntMatrix(this.value, newWidth);
	}

	public IntPair shape() {
		return IntPair.of(this.height, this.width);
	}

	public static void main(String[] args) {
		int size = 80;
		int w = 8;

		IntMatrix matrix = new IntMatrix(IntData.instance().rangeArray(960, 960 + size), w);

		System.out.println(matrix);

		System.out.println();
		System.out.println(matrix.shape());
		System.out.println();
		System.out.println(matrix.getAt(Slice.of(1, 5, 2), Slice.of(0, 4, 2)));
	}
}
