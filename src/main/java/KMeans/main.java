package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.Random;

public class main {

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] args1 = new GenericOptionsParser(conf, args).getRemainingArgs();

        /*
        args1[0] : input file
        args1[1] : output file
        args1[2] : k
        args1[3] : number of iterations
        args1[4] : point dimensions
         */

        // Check whether all the required parameters have been provided
        if (args1.length != 5) {
            System.err.println("Wrong usage of parameters");
            System.exit(-1);
        }

        int k = Integer.parseInt(args1[2]);
        conf.setInt("k",k);


        int dimensions = Integer.parseInt(args1[4]);
        conf.setInt("dimensions",dimensions);

        //set initial k random centroids
        Random rd = new Random();
        String point = "";
        System.out.println("Initial set of centroids:");
        for(int i=0;i<k;i++){
            for(int j=0;j<dimensions;j++){
                if(j==0) {
                    point = String.valueOf(rd.nextDouble() * 10D);
                    continue;
                }
                point = point.concat(","+rd.nextDouble()*10D);
            }
            conf.set("m"+i, point);
            System.out.println(i+"\t"+ conf.get("m"+i));
        }

        for(int i=0;i<Integer.parseInt(args1[3]);i++){
            System.out.println("------------------- Iteration number "+(i+1)+" -------------------");
            if(!createBuildClustersJob(conf,args1[0],args1[1]+"/iteration_"+(i+1)+"/"))
                System.exit(-1);

            //update the centroids for the next iteration
            updateCentroids(conf,args1[1]+"/iteration_"+(i+1)+"/");

        }

    }

    private static boolean createBuildClustersJob(Configuration conf, String inputFile, String outputFile) throws IOException, ClassNotFoundException, InterruptedException {

        final Job BuildClustersJob = new Job(conf,"BuildClusters");
        BuildClustersJob.setJarByClass(BuildClusters.class);


        // we set the classes of the mapper's output key and value
        BuildClustersJob.setMapOutputKeyClass(Text.class);
        BuildClustersJob.setMapOutputValueClass(Text.class);

        // we set the classes of the reducer's output key and value
        BuildClustersJob.setOutputKeyClass(Text.class);
        BuildClustersJob.setOutputValueClass(Text.class);

        // we set the mapper and the reducer classes
        BuildClustersJob.setMapperClass(BuildClusters.BuildClustersMapper.class);
        BuildClustersJob.setReducerClass(BuildClusters.BuildClustersReducer.class);

        // set the path of the input file and of the output
        FileInputFormat.setInputPaths(BuildClustersJob,new Path(inputFile));
        FileOutputFormat.setOutputPath(BuildClustersJob,new Path(outputFile));

        return BuildClustersJob.waitForCompletion(true);

    }

    private static void updateCentroids(Configuration conf, String outputFile) throws IOException {

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputFile+"part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        String point;
        line = br.readLine();
        while (line != null) {
            point = line.split("\t")[1];
            conf.set("m"+line.split("\t")[0],point);
            line = br.readLine();
        }

    }
}