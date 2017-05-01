package org.apache.carbondata.flink.exceptions;

import java.util.logging.Logger;

public class HadoopFormatException extends Exception {

    private String message;
    private final static Logger LOGGER = Logger.getLogger(HadoopFormatException.class.getName());

    public HadoopFormatException(String message) {
        this.message = message;
        LOGGER.info(message);
    }

}
