/*
 * Copyright (c) 2014 by its authors. Some rights reserved.
 * See the project homepage at
 *
 *     http://www.monifu.org/
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package monifu.concurrent.atomic.padded

import monifu.concurrent.atomic.AtomicNumberTest

class PaddedAtomicDoubleTest extends AtomicNumberTest[Double, AtomicDouble](
  "PaddedAtomicDouble", Atomic.builderFor(0.0), 17.23, Some(Double.NaN), Double.MaxValue, Double.MinValue) {

  describe("AtomicDouble") {
    it("should store MinPositiveValue, NaN, NegativeInfinity, PositiveInfinity") {
      assert(Atomic(Double.MinPositiveValue).get === Double.MinPositiveValue)
      assert(Atomic(Double.NaN).get.isNaN === true)
      assert(Atomic(Double.NegativeInfinity).get.isNegInfinity === true)
      assert(Atomic(Double.PositiveInfinity).get.isPosInfinity === true)
    }

    it("should countDownToZero(1.1)") {
      val r = Atomic(15.0)
      var decrements = 0
      var number = 0.0
      var continue = true

      while (continue) {
        val result = r.countDownToZero(1.5)
        continue = result > 0
        if (continue) {
          decrements += 1
          number += result
        }
      }

      assert(decrements === 10)
      assert(number === 15.0)
    }
  }
}

class PaddedAtomicFloatTest extends AtomicNumberTest[Float, AtomicFloat](
  "PaddedAtomicFloat", Atomic.builderFor(0.0f), 17.23f, Some(Float.NaN), Float.MaxValue, Float.MinValue) {

  describe("AtomicFloat") {
    it("should store MinPositiveValue, NaN, NegativeInfinity, PositiveInfinity") {
      assert(Atomic(Float.MinPositiveValue).get === Float.MinPositiveValue)
      assert(Atomic(Float.NaN).get.isNaN === true)
      assert(Atomic(Float.NegativeInfinity).get.isNegInfinity === true)
      assert(Atomic(Float.PositiveInfinity).get.isPosInfinity === true)
    }

    it("should countDownToZero(1.1)") {
      val r = Atomic(15.0f)
      var decrements = 0f
      var number = 0.0f
      var continue = true

      while (continue) {
        val result = r.countDownToZero(1.5f)
        continue = result > 0
        if (continue) {
          decrements += 1
          number += result
        }
      }

      assert(decrements === 10)
      assert(number === 15.0f)
    }
  }
}

class PaddedAtomicLongTest extends AtomicNumberTest[Long, AtomicLong](
  "PaddedAtomicLong", Atomic.builderFor(0L), -782L, None, Long.MaxValue, Long.MinValue)

class PaddedAtomicIntTest extends AtomicNumberTest[Int, AtomicInt](
  "AtomicInt", Atomic.builderFor(0), 782, None, Int.MaxValue, Int.MinValue)

class PaddedAtomicShortTest extends AtomicNumberTest[Short, AtomicShort](
  "PaddedAtomicShort", Atomic.builderFor(0.toShort), 782.toShort, None, Short.MaxValue, Short.MinValue)

class AtomicByteTest extends AtomicNumberTest[Byte, AtomicByte](
  "PaddedAtomicByte", Atomic.builderFor(0.toByte), 782.toByte, None, Byte.MaxValue, Byte.MinValue)

class PaddedAtomicCharTest extends AtomicNumberTest[Char, AtomicChar](
  "PaddedAtomicChar", Atomic.builderFor(0.toChar), 782.toChar, None, Char.MaxValue, Char.MinValue)

class AtomicNumberAnyTest extends AtomicNumberTest[BigInt, AtomicNumberAny[BigInt]](
  "PaddedAtomicNumberAny", Atomic.builderFor(BigInt(0)), BigInt(Int.MaxValue).toChar, None, BigInt(Long.MaxValue), BigInt(Long.MinValue))
