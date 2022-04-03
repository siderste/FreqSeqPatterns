package org.apache.spark.mllib.clustering.meanshift;

public interface DoubleDoubleVectorFunction {

    /**
     * Calculates the result of the left and right value of two vectors at a given
     * index.
     */
    public double calculate(int index, double left, double right);

}
