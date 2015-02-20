/**
 *
 */
package org.jongo;

/**
 * Copyright 2015 Cinovo AG<br>
 * <br>
 * Indicates that this object has a version
 *
 * @author yschubert
 *
 */
public interface IWithVersion {
	
	/**
	 *
	 * @return the version of the document
	 */
	Integer getDocumentVersion();

	/**
	 *
	 * @param version set the current version of the object
	 */
	void setDocumentVersion(Integer version);
	
}
