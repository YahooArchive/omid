package com.yahoo.omid.tm;

public class RollbackException extends Exception {
    
	private static final long serialVersionUID = -9163407697376986830L;

	public RollbackException(String message) {
		super(message);
	}

    public RollbackException() {
        super();
    }
}
