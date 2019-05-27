package com.aleite.elasticsearchTransform;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class elasticSearchTransformer {
    private String[][] fields;
    private String recordType;
    private String inDirectory;
    private String outDirectory;
    private String indexName;
    private boolean indexNameSet;
    private boolean recordTypeSet;
    private boolean dataTypesSet;
    private boolean outDirectorySet;
    private boolean inDirectorySet;
    private boolean threadPoolSizeSet;
    private ThreadPoolExecutor executor;
    private char delimiter;

    public elasticSearchTransformer() {
        this.recordTypeSet = false;
        this.dataTypesSet = false;
        this.outDirectorySet = false;
        this.threadPoolSizeSet = false;
    }

    public void setThreadPoolSize(int size) {
        this.executor = ((ThreadPoolExecutor) Executors.newFixedThreadPool(size));
        this.threadPoolSizeSet = true;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
        this.indexNameSet = true;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
        this.recordTypeSet = true;
    }
    
    public void setDataDelimiter(char delimiter){
        this.delimiter= delimiter;
    }

    public boolean loadDataTypes(String dataTypeFile, String mappingFile) {
        boolean status = false;
        if (this.recordTypeSet) {
            elasticSearchMapper mapper = new elasticSearchMapper();
            mapper.loadDataTypes(dataTypeFile);
            this.fields = mapper.readMapping(mappingFile, this.recordType);
            if ((this.fields != null) && (this.fields.length > 0)) {
                this.dataTypesSet = true;
                status = true;
                System.out.println("Map Size (" + mappingFile + "): " + this.fields.length);
            }
        }
        return status;
    }

    public void setInDirectory(String inDirectory) {
        this.inDirectory = inDirectory;
        this.inDirectorySet = true;
    }

    public void setOutDirectory(String outDirectory) {
        this.outDirectory = outDirectory;
        this.outDirectorySet = true;
    }

    public boolean transformDataFromFile(String file) {
        boolean status = false;
        if ((this.indexNameSet) && (this.recordTypeSet) && (this.dataTypesSet) && (this.inDirectorySet) && (this.outDirectorySet) && (this.threadPoolSizeSet)) {
            elasticSearchThread thread = new elasticSearchThread(file, this.inDirectory, this.outDirectory, this.indexName, this.recordType, this.fields, this.delimiter);
            this.executor.execute(thread);

            status = true;
        }
        return status;
    }

    public void stopThreads() {
        while (this.executor.getActiveCount() > 0) {
        }
        this.executor.shutdown();
        while (!this.executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }

}
