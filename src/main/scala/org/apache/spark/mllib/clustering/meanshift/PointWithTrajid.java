package org.apache.spark.mllib.clustering.meanshift;

import scala.Tuple2;

import java.io.Serializable;

public class PointWithTrajid implements Serializable {
    DoubleVector vec;
    long id;

    public PointWithTrajid(DoubleVector vec, long id) {
        this.vec = vec;
        this.id = id;
    }

    public Tuple2 toTupple(){
        Tuple2 result = new Tuple2 (this.id, this.vec.toArray());
        return result;
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
