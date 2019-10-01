/**
  * @author marinapopova
  * Sep 28, 2019
 */
package org.elasticsearch.kafka.indexer.exception;

public class ConsumerRecoverableException extends Exception {

	/**
	 * 
	 */
	public ConsumerRecoverableException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public ConsumerRecoverableException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public ConsumerRecoverableException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public ConsumerRecoverableException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ConsumerRecoverableException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
