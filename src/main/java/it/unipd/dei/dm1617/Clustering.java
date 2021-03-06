package it.unipd.dei.dm1617;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.Serializable;
import java.util.Random;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

public class Clustering implements Serializable {

    private ArrayList<Point> P; // Points that belong to the clustering
    private ArrayList<Cluster> clusters; // Clusters that belong to the clustering
    private HashMap<Point, Cluster> map;

    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, ArrayList<Point> centers, HashMap<Point, Cluster> map) {
        this.P = P;
        this.clusters = clusters;
        this.setCenters(centers);
        this.map = map;
    }

    public Clustering(ArrayList<Point> P, ArrayList<Cluster> clusters, ArrayList<Point> centers) {
        this.P = P;
        this.clusters = clusters;
        this.setCenters(centers);
        this.map = this.init(clusters);//Suppongo che cluster abbia tutti i punti e quindi posso creare il map
    }

    public ArrayList<Point> getPoints() {
        return P;
    }

    public ArrayList<Cluster> getClusters() {
        return clusters;
    }

    public Cluster getCluster(Point p) {
        return map.get(p);
    }

    public ArrayList<Point> getCenters() {
        ArrayList<Point> centroids = new ArrayList<Point>();
        for (Cluster c : clusters) {
            centroids.add(c.getCenter());
        }
        return centroids;
    }

    public ArrayList<Point> getCentroids() {
        ArrayList<Point> centroids = new ArrayList<Point>();
        for (Cluster c : clusters) {
            centroids.add(c.calculateCentroid());
        }
        return centroids;
    }

    public int getK() {
        return this.clusters.size();
    }

    public int getM() {
        return clusters.get(0).getPoints().get(0).parseVector().size();
    }

    public int size() {
        return this.P.size();
    }

    public Point getRandom() {
        Random rand = new Random();
        Point randomElement = this.P.get(rand.nextInt(this.P.size()));
        return randomElement;
    }

    public boolean addPoint(Point p, Cluster c) {
        if (!map.containsKey(p)) {
            P.add(p);
            map.put(p, c);
            c.getPoints().add(p);
            return true;
        } else {
            return false;
        }
    }

    public void addCluster(Cluster cl) {
        //ipotizzo che i punti dentro al cluster
        //siano gia dentro all'ArrayList di Punti del Clustering
        clusters.add(cl);
        ArrayList<Point> points = cl.getPoints();
        for (Point p : points) {
            map.put(p, cl);//per come ci serve va bene così;
            
        }
    }

    public void assignPoint(Point p, Cluster c) 
    {
        if(!map.get(p).equals(c))
        {
            map.get(p).getPoints().remove(p);
            map.put(p, c);
            c.getPoints().add(p);
        }
    }

    public void removePoint(Point p) {
        P.remove(p);
        Cluster cl = map.get(p);
        cl.getPoints().remove(p);
        map.remove(p);
    }

    // setPoints e setClusters forse conviene fare un clustering o rieseguire un init
    public double kmeans() {
        double sum = 0;
        for (int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for (int l = 0; l < points.size(); l++) {
                sum += Math.pow(Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector()), 2);
            }
        }
        //System.out.println("sum: "+sum);
        return sum;
    }

    //esegue solo cluster in parallelo, non è proprio completamente parallelizzato,diciamo che è una via di mezzo ma vabbe'
    public double kmeansMR(JavaSparkContext sc) {
        double sum = 0;
        for (Cluster C : getClusters()) {
            JavaRDD<Point> parallelize = sc.parallelize(C.getPoints());
            Broadcast<Point> b = sc.broadcast(C.getCenter());
            double kmeansMR = Cluster.kmeansMR(parallelize, b);
            sum += kmeansMR;
        }
        return sum;
    }

    public double kcenter() {
        double max = Distance.calculateDistance(this.clusters.get(0).getCenter().parseVector(),
                this.clusters.get(0).getPoints().get(0).parseVector());
        for (int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for (int l = 0; l < points.size(); l++) {
                double dist = Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector());
                if (dist > max) {
                    max = dist;
                }
            }
        }
        return max;
    }

    public double kmedian() {
        double sum = 0;
        for (int j = 0; j <= this.clusters.size() - 1; j++) {
            Cluster C = this.clusters.get(j);
            ArrayList<Point> points = C.getPoints();
            Point centroid = C.getCenter();
            for (int l = 0; l < points.size(); l++) {
                sum += Distance.calculateDistance(centroid.parseVector(), points.get(l).parseVector());
            }
        }
        return sum;
    }

    public double objectiveFunction(String objectiveFunction) {
        if (objectiveFunction.equals("kcenter")) {
            return this.kcenter();
        } else if (objectiveFunction.equals("kmeans")) {
            return this.kmeans();
        } else if (objectiveFunction.equals("kmedian")) {
            return this.kmedian();
        }
        return -1;
    }

    public HashMap<Point, Cluster> init(ArrayList<Cluster> clusters) {
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
        for (Cluster cl : clusters) {
            for (Point p : cl.getPoints()) {
                map.putIfAbsent(p, cl);
            }
        }
        return map;
    }

    //  clusters must be passed ordered
    public void setCenters(HashMap<Cluster, Point> mapCenters) {
        for (Cluster c : mapCenters.keySet()) {
            c.setCenter(mapCenters.get(c));

        }
    }

    public void setCenters(ArrayList<Point> centers) {
        for (int i = 0; i < centers.size(); i++) {
            Cluster c = clusters.get(i);
            c.setCenter(centers.get(i));

        }
    }

  //Nota bene: copia solo le liste ma se modifichi punti o i clusters le modifiche le fai anche su quello originale
   public Clustering clone()
    {
        ArrayList<Point> Pclone=(ArrayList<Point>) P.clone();
        HashMap<Point, Cluster> mapClone=(HashMap<Point, Cluster>) map.clone();
        ArrayList<Cluster> clustersClone=(ArrayList<Cluster>) clusters.clone();
        Clustering Cclone=new Clustering(Pclone,clustersClone,(ArrayList<Point>)getCenters().clone(),mapClone);
        return Cclone;
    }
    public Clustering copy() 
    {
        
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        HashMap<Point, Cluster> mapCopy = new HashMap<Point, Cluster>();
        ArrayList<Point> Scopy=Utility.copy(getCenters());
        ArrayList<Point> Pcopy=new ArrayList<Point>(P.size());
        for (int i = 0; i < getK(); i++) {
            Cluster C = new Cluster();
            C.setCenter(Scopy.get(i));
            clusters.add(C);     
        }
        
        for(int i=0;i<P.size();i++)
        {
            Point copy = P.get(i).copy();
            Pcopy.add(copy);
            int index = clusters.indexOf(map.get(P.get(i)));
            clusters.get(index).getPoints().add(copy);
            mapCopy.put(copy, clusters.get(index));  
        }
        Clustering Cclone = new Clustering(Pcopy, clusters, Scopy, mapCopy);
        return Cclone;
    }
    public HashMap<Point, Cluster> getMap() {
        return map;
    }

    public void reduceDim(SparkContext sc, int numComp) {
        ArrayList<Point> reducePointsDim = PCA.reducePointsDim(P, sc, numComp);
        for (int i = 0; i < P.size(); i++) {
            ((PointSpark) P.get(i)).assignVector(reducePointsDim.get(i).parseVector());
        }
        reducePointsDim = PCA.reducePointsDim(getCenters(), sc, numComp);
        for (int i = 0; i < reducePointsDim.size(); i++) {
            ((PointSpark) clusters.get(i).getCenter()).assignVector(reducePointsDim.get(i).parseVector());
        }
    }
    
    public void test()
    {
        int size = P.size();
        int sizeClusters=0;
        for(Cluster CL:clusters)
        {
            sizeClusters+=CL.size();
        }
        System.out.println("size: "+size+"   sizeClusters: "+sizeClusters);
    }

}
