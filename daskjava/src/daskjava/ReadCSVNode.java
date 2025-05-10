package daskjava;

import java.util.List;

public class ReadCSVNode extends TaskNode {
    private final String path;
    private final int numPartitions;

    public ReadCSVNode(String path) {
        this.path = path;
        this.numPartitions = Runtime.getRuntime().availableProcessors();
    }

    @Override
    public List<Partition> execute() {
        return CSVUtils.readAndPartitionCSV(path, numPartitions);
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public static void main(String[] args) {
        DaskDataFrame ddf = DaskDataFrame
                .readCSV("/Users/hoonit4/Documents/Projects/JDask/Bakery.csv");
        System.out.println(ddf);
    }
}
