package org.apache.spark.mllib.clustering.meanshift;

public interface DoubleVectorFunction {

    /**
     * Calculates the result with a given index and value of a vector.
     */
    public double calculate(int index, double value);

}
