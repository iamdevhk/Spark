// Import necessary Spark and Java libraries
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// Class for performing BFS based Shortest Path Search
public class ShortestPath {

    // Constants for node status
    public static final String ACTIVE = "ACTIVE";
    public static final String INACTIVE = "INACTIVE";

    // Main method for executing the BFS algorithm
    public static void main(String[] args) {
        // Retrieve input arguments: input file, start node, and end node
        String inputFile = args[0];
        String start = args[1];
        String end = args[2];

        // Configure Spark
        SparkConf conf = new SparkConf().setAppName("BFS-based Shortest Path Search");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile(inputFile);

        // Start timer
        long startTime = System.currentTimeMillis();

        // Create a network of nodes and their associated data
        JavaPairRDD<String, Data> network =
                lines.mapToPair(line -> {
                    // Parse input line to create node and its neighbors
                    int indexOfEq = line.indexOf("=");
                    String vertID = line.substring(0, indexOfEq );
                    String[] nlists = line.substring(indexOfEq + 1).split(";");
                    int countNodes = nlists.length;
                    List<Tuple2<String, Integer>> neighborNodes = new ArrayList<>();
                    for (int itr = 0; itr < countNodes; itr++) 
                    {
                        String[] parts = nlists[itr].split(",");
                        neighborNodes.add(new Tuple2<>(parts[0], Integer.parseInt(parts[1])));
                    }
                    // Initialize node data based on start node or others
                    Data nodedata;
                    if (vertID.equals(start))
                    {
                        nodedata = new Data(neighborNodes, 0, 0, ACTIVE);
                    } 
                    else
                    {
                        nodedata = new Data(neighborNodes, Integer.MAX_VALUE, Integer.MAX_VALUE, INACTIVE);
                    }
                    return new Tuple2<>(vertID,nodedata);
                });

        // Output the count of nodes in the network
        System.out.println("Count = " + network.count() + "\n");

        // Perform BFS until there are no more active nodes
        while (network.filter(checkerNode -> checkerNode._2.status.equals(ACTIVE)).count() > 0) 
        {
            // Propagate network to neighboring nodes
            JavaPairRDD<String, Data> propagateNtwrk = network.flatMapToPair(vertex -> {
                // Create Tuple2(neighbor, new Data()) for each neighbor if active
                List<Tuple2<String, Data>> listneighbor = new ArrayList<>();
                listneighbor.add(new Tuple2<>(vertex._1, new Data(vertex._2.neighbors, vertex._2.distance, vertex._2.prev, INACTIVE)));
                if (vertex._2.status.equals(ACTIVE)) 
                {
                    for (Tuple2<String, Integer> neighbor : vertex._2.neighbors) 
                    {
                        listneighbor.add(new Tuple2<>(neighbor._1, new Data(new ArrayList<>(), (neighbor._2 + vertex._2.distance), Integer.MAX_VALUE, INACTIVE)));
                    }
                }
                return listneighbor.iterator();
            });

           
            // Reduce by key to find the shortest distance for each node
            network = propagateNtwrk.reduceByKey((dataNode1, dataNode2) -> {
                // Update node data with the shortest distance and previous values
                List<Tuple2<String, Integer>> neighbors = dataNode1.neighbors.size() == 0 ? dataNode2.neighbors : dataNode1.neighbors;
                int dist = Math.min(dataNode1.distance, dataNode2.distance);
                int prev = Math.min(dataNode1.prev, dataNode2.prev);
                return new Data(neighbors, dist, prev, INACTIVE);
            });

            // Update node values based on the shortest distance
            network = network.mapValues(value -> {
                // Activate node status if new distance is shorter than previous
                if (value.distance < value.prev) 
                {
                    return new Data(value.neighbors, value.distance, value.distance, ACTIVE);
                }
                return value;
            });
        }

        // Retrieve the result for the end node
        List<Data> res = network.lookup(end);
        // Output the shortest distance from start to end node
        System.out.println("from " + start + " to " + end + " takes distance = " + res.get(0).distance);
        // Output the total execution time
        System.out.println("Time = " + (System.currentTimeMillis() - startTime));
    }

    // Class to hold data associated with each node in the graph given in the question pdf
    static class Data implements Serializable {
        List<Tuple2<String, Integer>> neighbors; // <neighbor0, weight0>, ...
        String status; // "INACTIVE" or "ACTIVE"
        Integer distance; // distance so far from source to this vertex
        Integer prev; // distance calculated in the previous iteration

        // Default constructor
        public Data() {
            neighbors = new ArrayList<>();
            status = "INACTIVE";
            distance = 0;
        }

        // Parameterized constructor
        public Data(List<Tuple2<String, Integer>> neighbors, Integer dist, Integer prev, String status) {
            if (neighbors != null) {
                this.neighbors = new ArrayList<>(neighbors);
            } else {
                this.neighbors = new ArrayList<>();
            }
            this.distance = dist;
            this.prev = prev;
            this.status = status;
        }
    }
}
