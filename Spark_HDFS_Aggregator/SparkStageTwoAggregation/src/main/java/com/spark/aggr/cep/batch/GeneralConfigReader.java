package com.spark.aggr.cep.batch;

public class GeneralConfigReader {

    private static GeneralConfigReader configReader;
    private static String hdfsUri=null;
    private GeneralConfigReader() {
    }

    public static GeneralConfigReader getInstance() {
        if (configReader == null) {
            synchronized (GeneralConfigReader.class) {
                if (configReader == null) {
                    configReader = new GeneralConfigReader();
                }
            }
        }
        return configReader;
    }
    
    public String getHdfsUriProperty() throws Exception {
        return hdfsUri;
    }
    public void setHdfsUriProperty(String hdfsUri) {
    	this.hdfsUri=hdfsUri;
    }

    public String getHdfsUser() throws Exception {
        String hdfsUser = "root";
        return hdfsUser;
    }

}
