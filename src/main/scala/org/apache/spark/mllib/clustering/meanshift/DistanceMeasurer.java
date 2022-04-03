package org.apache.spark.mllib.clustering.meanshift;

public interface DistanceMeasurer {

    public double measureDistance(double[] set1, double[] set2);

    public double measureDistance(DoubleVector vec1, DoubleVector vec2);

}
