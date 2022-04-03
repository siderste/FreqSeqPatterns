package semantics;

import org.apache.commons.math3.util.FastMath;
import java.util.*;

import org.apache.spark.mllib.clustering.meanshift.DenseDoubleVector;
import org.apache.spark.mllib.clustering.meanshift.DoubleVector;
import org.apache.spark.mllib.clustering.meanshift.EuclidianDistance;
import org.apache.spark.mllib.clustering.meanshift.PointWithTrajid;

public class LocalMeanShiftGivenDistancesJava {

    private final double SQRT_2_PI = FastMath.sqrt(2 * Math.PI);

    int maxIteration = 100;
    double bandwidth = 0.01;
    double mergeWindow = 0.001;
    List<Integer> weights = null; // weights of points
    List<PointWithTrajid> points = null;
    List<DoubleVector> centers = null;
    boolean[] converged = null;
    int[] designation = null;//same size as points

    public List<DoubleVector> cluster(List<PointWithTrajid> pointsAndDistances, List<Integer> weights,
                                      double bandwidth, double mergeWindow, int maxIterations) {

        int k = 0;
        initialize(points, weights, maxIterations, bandwidth, mergeWindow);

        // now iterate over all centers
        for (int i = 0; i < maxIterations; i++) {
            k = i+1;
            // return the number of centers that have not converged yet
            int remainingConvergence = meanShift();
            // merge if centers are within the mergeWindow

            if (remainingConvergence == 0) {
                break;
            }
        }
        System.out.println("MS iterations="+k);
        assign();
        return centers;
    }

    //get clusters from designation
    public Map<Integer, List<PointWithTrajid>> getClusters() {
        Map<Integer, List<PointWithTrajid>> clusters = new HashMap<Integer, List<PointWithTrajid>>();
        //key:cluster id, value:list of members
        for(int i=0; i<designation.length; i++) {
            int clusterID = designation[i];
            if ( clusters.containsKey(clusterID) ) {
                clusters.get( clusterID ).add(points.get(i));
            } else {
                List<PointWithTrajid> newCluster = new ArrayList<PointWithTrajid>();
                newCluster.add( points.get(i) );
                clusters.put( clusterID, newCluster );
            }
        }
        return clusters;
    }

    public int[] getDesignation() {
        return designation;
    }


    /*
     *
     * */
    private void initialize(List<PointWithTrajid> points, List<Integer> weights, int maxIteration,
                            double bandwidth, double mergeWindow) {

        this.maxIteration = maxIteration;
        this.bandwidth = bandwidth;
        this.mergeWindow = mergeWindow;

        this.points = points;
        this.weights = weights;
        findInitialCenters();

        this.converged = new boolean[points.size()];
        Arrays.fill(this.converged, false);

        this.designation = new int[points.size()];
        Arrays.fill(this.designation, -1);

    }

    private void findInitialCenters() {
        centers = new ArrayList<DoubleVector>();
        for (int i = 0; i < points.size(); i++) {
            DoubleVector point = points.get(i).getVec();
            DoubleVector center = new DenseDoubleVector(point);
            centers.add(center);
        }
    }

    private int meanShift() {
        int remainingConvergence = 0;
        for (int i = 0; i < centers.size(); i++) {
            // only update the centers that have not converged yet.
            if (converged[i]) {
                continue;
            }
            DoubleVector v = centers.get(i);
            List<VectorDistanceTuple> neighbours = getNeighbors(v);
            DoubleVector numerator = new DenseDoubleVector(v.getLength());
            double denominator = 0;
            for (VectorDistanceTuple neighbour : neighbours) {
                double weight = weights.get( neighbour.getId() );
                double normDistance = neighbour.getDistance() / bandwidth;
                double gradient = -gaussianGradient(normDistance) * weight;
                numerator = numerator.add(neighbour.getVector().multiply(gradient));
                denominator += gradient;
            }
            if (denominator > 0d) {
                DoubleVector newCenter = numerator.divide(denominator);
                if (v.subtract(newCenter).abs().sum() > 1e-4) {
                    remainingConvergence++;
                } else {
                    converged[i] = true;
                }
                // apply the shift
                centers.set(i, newCenter);
            }
        }
        return remainingConvergence;
    }

    private List<VectorDistanceTuple> getNeighbors( DoubleVector vec ) {
        List<VectorDistanceTuple> results = new ArrayList<LocalMeanShiftGivenDistancesJava.VectorDistanceTuple>();
        for(int i=0; i<points.size(); i++) {
            DoubleVector otherVector = points.get( i ).getVec();
            double dist = EuclidianDistance.get().measureDistance(vec, otherVector);
            if( dist <= bandwidth ) {
                VectorDistanceTuple tuple = new VectorDistanceTuple(otherVector, i, dist);
                results.add( tuple );
            }
        }
        return results;
    }

    private double gaussianGradient(double dist) {
        return -1;
        //return - FastMath.exp(-(dist * dist) / 2d) * dist / SQRT_2_PI ;
    }

    private void assign() {

        int totalPointNum = points.size();
        boolean [] assigned = new boolean[ totalPointNum ];
        Arrays.fill(assigned, false);

        //Loop through and assign clusters
        int curClusterID = 0;
        boolean progress = true;
        while(progress) {
            progress = false;
            int basePos = 0;//This will be the mode of our cluster
            while( basePos < totalPointNum && assigned[basePos]  )
                basePos++;
            for(int i = basePos; i < totalPointNum; i++) {
                if( assigned[i] )
                    continue;//Already assigned
                progress = true;
                double dist = EuclidianDistance.get().measureDistance(centers.get(basePos), centers.get(i));
                if( dist < mergeWindow ) {
                    assigned[i] = true;
                    designation[i] = curClusterID;
                }
            }

            curClusterID++;
        }
    }

    private class VectorDistanceTuple {

        final DoubleVector vec;
        final int id;
        final double dist; //distance of the vector to the target vector

        public VectorDistanceTuple(DoubleVector vec, int id, double dist) {
            this.vec = vec;
            this.id = id;
            this.dist = dist;
        }

        public double getDistance() {
            return dist;
        }

        public int getId() {
            return id;
        }

        public DoubleVector getVector() {
            return vec;
        }

        @Override
        public String toString() {
            return vec + " -> " + dist;
        }
    }

}
