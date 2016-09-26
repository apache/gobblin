package gobblin.util.request_allocation;

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
