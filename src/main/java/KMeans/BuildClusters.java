package KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class BuildClusters {


    public static class BuildClustersMapper extends Mapper<Object, Text, Text, Text>{

        int k;
        int dimensions;
        int index;
        double min;
        double distance;
        ArrayList<Double> point;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k",2);
            dimensions = conf.getInt("dimensions",2);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            point = new ArrayList<>();
            for(int i=0;i<dimensions;i++){
                point.add(i,Double.parseDouble(value.toString().split(",")[i]));
            }

            min = 1000000d; //infinite

            for(int i=0;i<k;i++){
                distance = 0d;
                for(int j=0;j<dimensions;j++){
                    distance += Math.pow((point.get(j) - Double.parseDouble(conf.get("m"+i).split(",")[j])),2);
                }
                if (distance < min){
                    min = distance;
                    index = i;
                }
            }

            // emit the cluster the point belongs to (the closest centroid), in the form <cluster id,point>
            context.write(new Text(String.valueOf(Integer.valueOf(index))),value);

        }

        /* Make sure to emit all the k centroids (there could be clusters without any point). Emit all the k centroids with a "null" value, just to retrieve them on the reducer side */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k",2);
            for(int i=0;i<k;i++){
                context.write(new Text(String.valueOf(i)),new Text("null"));
            }
        }
    }

    public static class BuildClustersReducer extends Reducer<Text, Text, Text, Text>{

        ArrayList<Double> sum;
        int clusterCardinality;
        int k;
        int dimensions;
        String newCentroid;
        boolean first;

        @Override
        protected void setup(Context context){
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k",2);
            dimensions = conf.getInt("dimensions",2);
        }

        // for each one of the k centroids, receive a list of point that belong to that cluster and one or more "null" values.
        // if a centroid has associated only "null" values, it means that no point belong to that cluster: that centroid must be emitted unmodified from the previous iteration
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            sum = new ArrayList<>();
            clusterCardinality = 0;
            first = true;
            for(Text p:values){
                if(p.toString().equals("null"))
                    continue;
                if(first){
                    for(int j=0;j<dimensions;j++){
                        sum.add(j,Double.parseDouble(p.toString().split(",")[j]));
                    }
                    first = false;
                }
                else{
                    for(int j=0;j<dimensions;j++){
                        sum.set(j,sum.get(j)+Double.parseDouble(p.toString().split(",")[j]));
                    }
                }
                clusterCardinality++;
            }

            //if the centroid has no point associated, its cardinality at this point is equal to 0
            //emit the centroid unmodified from the previous iteration
            if(clusterCardinality == 0) {
                context.write(key,new Text(conf.get("m"+key)));
            }
            else{
                //emit new centroid for the "key" cluster
                newCentroid = "";
                for(int i=0;i<dimensions;i++){
                    if (i==0) {
                        newCentroid = String.valueOf(sum.get(i)/clusterCardinality);
                        continue;
                    }
                    newCentroid = newCentroid.concat(","+sum.get(i)/clusterCardinality);
                }
                context.write(key,new Text(newCentroid));
            }

        }

    }

}

