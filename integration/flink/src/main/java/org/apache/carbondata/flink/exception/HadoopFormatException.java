package org.apache.carbondata.flink.exception;
        import java.util.logging.Logger;


public class HadoopFormatException extends Exception {

    private final static Logger LOGGER = Logger.getLogger(HadoopFormatException.class.getName());
    private String message;
    public HadoopFormatException(String message){
        this.message = message;
        LOGGER.info(message);
    }
}
