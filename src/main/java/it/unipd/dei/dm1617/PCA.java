/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import Jama.EigenvalueDecomposition;
import breeze.linalg.DenseMatrix;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author DavideDP
 */
public class PCA {

    public static ArrayList<Point> PCAPoints(ArrayList<Point> P, SparkContext sc, int numComp, boolean optimal, boolean normalize) throws Exception {
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        //Trasformo punti in vettori
        ArrayList<Vector> PCAs = PCA.PCA(vectors, sc, numComp, optimal, normalize);
        //Trasformo vettori in punti, lo uso solo se voglio stampare il clustering fatto in 2d
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        for (Vector v : PCAs) //vectors
        {
            pointsPCA.add(new PointSpark(v));
        }
        return pointsPCA;
    }

    public static ArrayList<Vector> PCA(List<Vector> rowsList, SparkContext sc) throws Exception {
        return PCA.PCA(rowsList, sc, 2, false, true);
    }

    //Data una lista di vettori restituisce i vettori dopo la PCA
    //Se optimal=true allora sceglie il valore ottimale
    //Se normalize è true allora fa la normalizzazione
    public static ArrayList<Vector> PCA(List<Vector> rowsList, SparkContext sc, int numComp, boolean optimal, boolean normalize) throws Exception {
        if (normalize) {
            rowsList = normalize2(rowsList, sc);
        }
        JavaRDD<Vector> rows = JavaSparkContext.fromSparkContext(sc).parallelize(rowsList);
        // Create a RowMatrix from JavaRDD<Vector>.
        RowMatrix mat = new RowMatrix(rows.rdd());
        if (optimal) {
            numComp = optimalNumComp(mat);
        }
        System.err.println("STOP");
        Matrix pc = mat.computePrincipalComponents(numComp);
        RowMatrix projected = mat.multiply(pc);
        long numRows = projected.numRows();
        System.out.println(numRows);
        //dobbiamo ancora fare la normalizzazione :(
        DenseMatrix<Object> toBreeze = projected.toBreeze();
        ArrayList<Vector> vecs = new ArrayList<Vector>();
        for (int i = 0; i < numRows; i++) {
            double[] d = new double[numComp];
            for (int j = 0; j < numComp; j++) {
                d[j] = (Double) toBreeze.apply(i, j);
            }
            Vector v = Vectors.dense(d);
            vecs.add(v);
        }
        return vecs;
    }

    public static Clustering PCAClustering(Clustering C, JavaSparkContext sc, int numComp, boolean normalize) throws Exception {
        ArrayList<Point> P = C.getPoints();
        ArrayList<Point> centers = C.getCenters();
        //Creo vettori
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        //Trasformo vettori
        ArrayList<Vector> PCAs = PCA.PCA(vectors, sc.sc(), numComp, false, true);
        //Creo punti
        ArrayList<Point> pointsPCA = new ArrayList<Point>();
        ArrayList<Point> centersPCA = new ArrayList<Point>();
        for (int i = 0; i < PCAs.size(); i++) //vectors
        {
            Point add = new PointSpark(vectors.get(i));
            if (centers.contains(P.get(i))) {
                centersPCA.add(add);
            }
            pointsPCA.add(add);
        }
        ArrayList<Cluster> clusters = new ArrayList<Cluster>();
        HashMap<Point, Cluster> map = new HashMap<Point, Cluster>();
        for (int i = 0; i < C.getK(); i++) {
            Cluster CL = new Cluster();
            CL.setCenter(centers.get(i));
            clusters.add(CL);
        }
        for (int i = 0; i < P.size(); i++) {
            Cluster get = C.getMap().get(P.get(i));
        }
        return null;
    }

    public static List<Vector> normalize2(List<Vector> rowsList, SparkContext sc) {
        for (int i = 0; i < rowsList.size(); i++) {
            //System.out.println("PRIMA: "+rowsList.get(i));
            BLAS.scal(1 / Vectors.norm(rowsList.get(i), 2), rowsList.get(i));
            rowsList.set(i, rowsList.get(i));
            //System.out.println("DOPO: "+rowsList.get(i));
        }
        return rowsList;
    }

    public static List<Vector> normalize(List<Vector> rowsList, SparkContext sc) {
        int id = 0;
        List<Row> data = new ArrayList<Row>();
        for (int i = 0; i < rowsList.size(); i++) {
            data.add(RowFactory.create(id, rowsList.get(i).asML()));
        }
        StructType schema = new StructType(new StructField[]{new StructField("id", DataTypes.IntegerType, false, Metadata.empty()), new StructField("features", new VectorUDT(), false, Metadata.empty())});
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> dataFrame = sqlContext.createDataFrame(data, schema);
        // Normalize each Vector using $L^1$ norm.
        Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("normFeatures").setP(1.0);
        Dataset<Row> l1NormData = normalizer.transform(dataFrame);
        //l1NormData.show();
        // Normalize each Vector using $L^\infty$ norm.
        Dataset<Row> lInfNormData = normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        //lInfNormData.show();
        List<Row> normalized = lInfNormData.collectAsList();
        /*List<Vector> normalizedData=new ArrayList<Vector>();
        for(int i=0;i<normalized.size();i++)
        {
        normalizedData.add( (Vector)normalized.get(i).apply(1));
        } */
        //return normalizedData;
        return null;
    }

    //trova il numero ottimale di componenti
    public static int optimalNumComp(RowMatrix mat) throws Exception {
        int numCols = (int) mat.numCols();
        int numRows = (int) mat.numRows();
        DenseMatrix<Object> toBreeze = mat.toBreeze();
        double[][] d = new double[numRows][numCols];
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                d[i][j] = (Double) toBreeze.apply(i, j);
            }
        }
        Jama.Matrix M = new Jama.Matrix(d);
        EigenvalueDecomposition e = M.eig();
        double[] Eigenvalues = e.getRealEigenvalues();
        ArrayList<Double> AL = new ArrayList<Double>();
        double sum = 0;
        for (int i = 0; i < Eigenvalues.length; i++) {
            if (Eigenvalues[i] > 0) {
                AL.add(Eigenvalues[i]);
                sum += Eigenvalues[i];
            }
        }
        double[] dd = new double[AL.size()];
        Object[] toArray = AL.toArray();
        for (int i = 0; i < toArray.length; i++) {
            dd[i] = (Double) toArray[i];
            //System.out.println(dd[i]/sum);
        }
        Arrays.sort(dd);
        double acc = 0;
        int cont = 0;
        for (int i = 0; i < dd.length; i++) {
            acc += dd[i];
            if (acc / sum < 0.95) {
                cont++;
            }
        }
        System.out.println("DimensioniIniziali: " + numCols + "  DimensioniFinali: " + cont);
        return cont;
    }

    public static ArrayList<Point> reducePointsDim(ArrayList<Point> P, SparkContext sc, int numComp) {
        ArrayList<Vector> vectors = new ArrayList<Vector>();
        for (Point p : P) {
            Vector v = p.parseVector();
            vectors.add(v);
        }
        ArrayList<Point> Pnew = new ArrayList<Point>();
        for (Vector v : vectors) //vectors
        {
            double[] d = new double[numComp];
            for (int i = 0; i < numComp; i++) {
                d[i] = v.apply(i);
            }
            Vector dense = Vectors.dense(d);
            Pnew.add(new PointSpark(dense));
        }
        return Pnew;
    }
    
}
