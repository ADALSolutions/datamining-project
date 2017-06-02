/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 *
 * @author DavideDP
 */
public class CentersBuilder {

    public static ArrayList<Point> initMedianCenters(ArrayList<Point> points, int k) {
        ArrayList<Point> P = (ArrayList<Point>) points.clone();
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        for (int i = 0; i < k; i++) {
            Cluster C = new Cluster();
            clusters.add(C);
        }
        Random r = new Random();
        int l = P.size() / k;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < l; j++) {
                int nextInt = r.nextInt(P.size());
                Point p = P.get(nextInt);
                clusters.get(i).getPoints().add(p);
                P.remove(nextInt);
            }
        }
        clusters.get(clusters.size() - 1).getPoints().addAll(P);
        for (int i = 0; i < k; i++) {
            Cluster C = clusters.get(i);
            C.setCenter(C.calculateCentroid());
        }
        ArrayList<Double> sums = new ArrayList<Double>();
        for (int i = 0; i < k; i++) {
            Cluster C = clusters.get(i);
            double sum = 0;
            for (int j = 0; j < C.getPoints().size(); j++) {
                double dist = Distance.calculateDistance(C.getPoints().get(j).parseVector(), C.getCenter().parseVector());
                C.getPoints().get(j).setDist(dist);
                sum += dist;
            }
            sums.add(sum);
        }
        ArrayList<Point> centers = new ArrayList<Point>();
        for (int i = 0; i < k; i++) {
            Cluster C = clusters.get(i);
            Object[] toArray = C.getPoints().toArray();
            Arrays.sort(toArray);
            double acc = 0;
            double sum = sums.get(i) / 2;
            for (int j = 0; j < toArray.length; j++) {
                acc += ((Point) toArray[j]).getDist();
                if (acc > sum) {
                    double diff1 = Math.abs(sum - (acc - ((Point) toArray[j]).getDist()));
                    double diff2 = Math.abs(acc - sum);
                    if (diff1 < diff2) {
                        centers.add((Point) toArray[j - 1]);
                    } else {
                        centers.add((Point) toArray[j]);
                    }
                    break;
                }
            }
        }
        return centers;
    }

    public static ArrayList<Point> getRandomCenters(int dim, int k) {
        ArrayList<Point> S = new ArrayList<Point>(k);
        for (int i = 0; i <= k - 1; i++) {
            S.add(new PointSpark(Utility.toArrayList(Utility.generateRandomVector(dim).toArray())));
        }
        return S;
    }

    public static ArrayList<Point> kmeansPlusPlus(ArrayList<Point> P, int k) {
        Point c1 = CentersBuilder.getRandomCenters(P, 1).get(0);
        ArrayList<Point> S = new ArrayList<Point>();
        S.add(c1);
        P.remove(c1);
        ArrayList<Double> dP = new ArrayList<Double>(P.size());
        for (int i = 1; i < k; i++) {
            double sum = 0;
            //Calcolare dP
            for (int j = 0; j < P.size(); j++) {
                double min = Distance.calculateDistance(P.get(j).parseVector(), S.get(0).parseVector());
                for (int l = 0; l < S.size(); l++) {
                    double dist = min = Distance.calculateDistance(P.get(j).parseVector(), S.get(l).parseVector());
                    if (dist < min) {
                        min = dist;
                    }
                }
                dP.add(j, min);
                sum += Math.pow(min, 2);
            }
            //Calcolo Dp/sum
            for (int j = 0; j < P.size(); j++) {
                dP.set(j, Math.pow(dP.get(j), 2) / sum);
            }
            //Scelgo a random un punto c_i
            double threshold = Math.random();
            double sum2 = 0;
            //continuo a iterare finchè la somma non supera la threshold
            //quando succede ho beccato il c_i che cerco
            for (int j = 0; j < P.size(); j++) {
                sum2 += dP.get(j);
                if (sum2 >= threshold) {
                    S.add(P.get(j));
                    P.remove(P.get(j));
                    break;
                }
            }
        }
        P.addAll(S);
        return S;
    }

    public static ArrayList<Point> getRandomCenters(ArrayList<Point> P, int k) {
        ArrayList<Point> S = new ArrayList<Point>(k);
        Random r = new Random();
        for (int i = 0; i <= k - 1; i++) {
            int index = r.nextInt(P.size());
            S.add(P.get(index));
        }
        return S;
    }
    
}
