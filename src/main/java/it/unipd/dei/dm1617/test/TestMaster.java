package it.unipd.dei.dm1617.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestMaster {

    public static void main(String args[]) throws IOException, InterruptedException 
    {
        System.out.println("MASTER");
        SparkConf conf = new SparkConf(true).setAppName("Sampler")
            .setMaster("spark://25.78.49.45:1080").set("spark.ui.port","1080");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        String master = sc.master();
        System.out.println("Master: "+master);
        
        //Init Server Socket
        ServerSocket server = null;
        int port = 8070;
        try {
            server = new ServerSocket(port);//(port,5,ia);
        } catch (IOException ex) {
            Logger.getLogger("");
            System.out.println("Server Socket non partito");
        }
        System.out.println("address: " + server.getInetAddress());
        System.out.println("Server partito sulla porta " + server.getLocalPort());
        
        //Cerco client
        Socket client = null;
        int cont=0;
        while (cont==0) {
            System.out.println("In attesa di connessioni...");
            try {
                client = server.accept();
                System.out.println("client : " + client.getInetAddress());
            } catch (IOException ex) {
                System.out.println("Errore");
            }

        }
        
        //Creo ArrayList
        ArrayList<Double> AL=new ArrayList<Double>();
        for(int i=0;i<1000000;i++)
        {
            AL.add(Math.random());
        }
        //Eseguo in parallelo
        JavaRDD<Double> parallelize = sc.parallelize(AL);
        JavaRDD<Double> map = parallelize.map((d)->2*d);
        map.cache();
        map.collect();
        map.saveAsTextFile("dataset\\mappando");
        while(true){}


    }
}
