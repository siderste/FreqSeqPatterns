package org.apache.spark.mllib.clustering.meanshift;

import java.io.Serializable;

public class PointWithTrajid implements Serializable {
    DoubleVector vec;
    long id;

    public PointWithTrajid(DoubleVector vec, long id) {
        this.vec = vec;
        this.id = id;
    }

    public DoubleVector getVec() {
        return vec;
    }

    public void setVec(DoubleVector vec) {
        this.vec = vec;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}
