/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.util.request_allocation;

/**
 * Basic vector operations. These operations are NOT safe (e.g. no bound checks on vectors), so they are package-private.
 */
class VectorAlgebra {

  /**
   * Performs x + cy
   */
  static double[] addVector(double[] x, double[] y, double c, double[] reuse) {
    if (reuse == null) {
      reuse = new double[x.length];
    }
    for (int i = 0; i < x.length; i++) {
      reuse[i] = x[i] + c * y[i];
    }
    return reuse;
  }

  /**
   * Performs c * x
   */
  static double[] scale(double[] x, double c, double[] reuse) {
    if (reuse == null) {
      reuse = new double[x.length];
    }
    for (int i = 0; i < x.length; i++) {
      reuse[i] = c * x[i];
    }
    return reuse;
  }

  /**
   * @return true if test is larger than reference in any dimension. If orequal is true, then return true if test is larger
   *         than or equal than reference in any dimension.
   */
  static boolean exceedsVector(double[] reference, double[] test, boolean orequal) {
    for (int i = 0; i < reference.length; i++) {
      if (reference[i] < test[i]) {
        return true;
      }
      if (orequal && reference[i] == test[i]) {
        return true;
      }
    }
    return false;
  }

}
