/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unipd.dei.dm1617;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author DavideDP
 */
public class BestK {
    
        
    public static Clustering bestkClustering_test(ArrayList<Point> P, String algorithm, BiFunction<Clustering, Clustering, Boolean> f) throws IOException {
        File file=new File("grafico.txt");
        FileWriter fw=null;
        fw = new FileWriter(file);
        int k = 1;
        Clustering C = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
        double score=C.kmeans();
        fw.write(""+k+" "+score+"\n");
        double phikprime = C.objectiveFunction(algorithm);
        k = 2;
        Clustering C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
        
        while (f.apply(C, C2)) 
        {
            double score2=C2.kmeans();
            double ratio=Math.abs((score-score2))/score;
            System.out.println("ratio: "+ratio);
            fw.write(""+k+" "+score2+"\n");
            //System.out.println("k attuale:"+k);
            //System.out.println("Improvement "+C.kmeans()/C2.kmeans());
            C = C2;
            k = k + 2;
            phikprime = C.objectiveFunction(algorithm);
            C2 = ClusteringBuilder.clusteringAlgorithm(P, k, f, algorithm);
        }
        double score2=C2.kmeans();
        double ratio=Math.abs((score-score2))/score;
        fw.write(""+k+" "+score2+"\n");
        System.out.println("ratio: "+ratio);
        int kprime = k / 2;
        fw.close();
        //return C2;
        return bisection(C, C2, kprime, k, phikprime, C2.objectiveFunction(algorithm), algorithm, f);
    }
    public static Clustering bestkClustering(ArrayList<Point> P, String algorithm, BiFunction<Clustering, Clustering, Boolean> conditionStop) {
        int k = 1;
        Clustering C = ClusteringBuilder.clusteringAlgorithm(P, k, conditionStop, algorithm);
        double score=C.kmeans();
        double phikprime = C.objectiveFunction(algorithm);
        k = 2;
        Clustering C2 = ClusteringBuilder.clusteringAlgorithm(P, k, conditionStop, algorithm);

        while (conditionStop.apply(C, C2)) 
        {
            System.out.println("BIC: "+XMeans.BIC(C));
            showDiff( score,C2);
            C = C2;
            k = k * 2;
            phikprime = C.objectiveFunction(algorithm);
            C2 = ClusteringBuilder.clusteringAlgorithm(P, k, conditionStop, algorithm);
        }

        int kprime = k / 2;
        //return C2;
        return bisection(C, C2, kprime, k, phikprime, C2.objectiveFunction(algorithm), algorithm, conditionStop);
    }

    public static Clustering bisection(Clustering C, Clustering C2, int kprime, int k, double phikprime, double phik, String algorithm, BiFunction<Clustering, Clustering, Boolean> f) {
        int kmedio = (int) (kprime + k) / 2;
        if (kmedio == kprime) {
            return C2;
        }
        Clustering Cmedio = ClusteringBuilder.clusteringAlgorithm(C.getPoints(), kmedio, f, algorithm);
        double phikmedio = Cmedio.objectiveFunction(algorithm);
        if (f.apply(Cmedio, C2)) {
            return bisection(Cmedio, C2, kmedio, k, phikmedio, phik, algorithm, f);
        } else {
            return bisection(C, Cmedio, kprime, kmedio, phikprime, phikmedio, algorithm, f);
        }
    }
    
    public static void showDiff(double score,Clustering C2)
    {
        double score2=C2.kmeans();
        double ratio=Math.abs((score-score2))/score;
        System.out.println("ratio: "+ratio);        
    }
    
}
