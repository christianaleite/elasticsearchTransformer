package com.aleite.elasticsearchTransform;

import com.aleite.utilities.fileSystem.fileSystemNavigator;

public class Main {

    public static void main(String[] args) {
        if (args.length == 8) {

            long startTime = System.nanoTime();

            int threadPoolSize = Integer.parseInt(args[0]);

            /* Load command line arguments */

            String indexName = args[1];
            String type = args[2];
            String dataTypeFile = args[3];
            String mappingFile = args[4];
            String inDirectory = args[5];
            String outDirectory = args[6];
            char delimiter = args[7].charAt(0);

            /*
             Verify if wildcard was included on the inDirectory and if found initialize fileSystemNavigator with prefix.
             This is used to filter which files will be processed.
            */

            fileSystemNavigator fileSystem = null;
            if (inDirectory.endsWith("*")) {
                String prefix = inDirectory.substring(inDirectory.lastIndexOf("/") + 1, inDirectory.length() - 1);
                inDirectory = inDirectory.substring(0, inDirectory.lastIndexOf("/") + 1);

                fileSystem = new fileSystemNavigator(inDirectory, prefix);
            } else {
                fileSystem = new fileSystemNavigator(inDirectory);
            }

            /* Obtain the list of files located in the inDirectory and continue only if files are found */

            String[] files = fileSystem.listFiles();
            if ((files != null) && (files.length > 0)) {

                elasticSearchTransformer processor = new elasticSearchTransformer();
                processor.setIndexName(indexName);
                processor.setInDirectory(inDirectory);
                processor.setOutDirectory(outDirectory);
                processor.setRecordType(type);
                processor.setThreadPoolSize(threadPoolSize);
                processor.setDataDelimiter(delimiter);

                /* Load the provided data types and the mapping file to be used to convert the format of all the files */

                if (processor.loadDataTypes(dataTypeFile, mappingFile)) {
                    boolean status;
                    for (int i = 0; i < files.length; i++) {

                        /* Queue files to be processed */

                        System.out.println("Queuing File: " + files[i]);
                        status = processor.transformDataFromFile(inDirectory + files[i]);
                        if (!status) {
                            System.out.println("[ERROR] Cannot Queue File: " + files[i]);
                        }
                    }
                } else {
                    System.out.println("Cannot load data types");
                }
                processor.stopThreads();
            }
            long endTime = System.nanoTime();

            long timeElapsed = endTime - startTime;

            System.out.println("Execution time in milliseconds : " + timeElapsed / 1000000L);
        }else{
            System.out.println("USO: [Number of Threads] [Index Name] [Record Type] [Record Type Config File] [Map Config File] [In Directory] [Out Directory] [Data Delimiter]");
        }
    }
}
