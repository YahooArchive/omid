package com.yahoo.omid.tm;

public class TransactionException extends Exception {

	private static final long serialVersionUID = 7273525983622126275L;

	public TransactionException(String reason) {
		super(reason);
	}

	public TransactionException(String reason, Throwable e) {
		super(reason, e);
	}
}
