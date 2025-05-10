package daskjava;

import java.io.*;
import java.util.*;

public class CSVUtils {
    private static final String DEFAULT_DIR = System.getProperty("user.dir");

    public static List<Partition> readAndPartitionCSV(String path, int numPartitions) {
        List<Map<String, String>> allRows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String headerLine = reader.readLine();
            if (headerLine==null) {
                return List.of();
            }
            String[] headers = headerLine.split(",");

            String line;
            while ((line = reader.readLine())!=null) {
                String[] rowData = line.split(",");
                Map<String, String> row = new HashMap<>();
                // need to account for less
                for (int i=0; i<rowData.length; ++i) {
                    row.put(headers[i].trim(), rowData[i].trim());
                }
                allRows.add(row);
            }
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + path);
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        List<Partition> partitions = new ArrayList<>();
        int chunkSize = Math.max(1, allRows.size()/numPartitions);
        for (int i=0; i<allRows.size(); i+=chunkSize) {
            partitions.add(new Partition(allRows.subList(i, Math.min(i+chunkSize, allRows.size()))));
        }
        return partitions;
    }
}
