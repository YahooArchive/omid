package com.yahoo.omid.transaction;

/**
 * @author Igor Katkov
 */
public class OmidInstantiationException extends ReflectiveOperationException{

    public OmidInstantiationException(String message, Throwable cause) {
        super(message, cause);
    }
}
