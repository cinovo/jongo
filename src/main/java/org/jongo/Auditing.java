package org.jongo;

import java.util.Date;

import org.bson.LazyBSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Copyright 2015 Cinovo AG<br>
 * <br>
 * Auditing methods
 *
 * @author yschubert
 *
 */
class Auditing {
	
	/**
	 * Name of the version field
	 */
	public static final String VERSION_FIELD = "_version";
	
	/**
	 * Name of the field indicating the last change of the document
	 */
	public static final String LASTCHANGE_FIELD = "_lastChange";
	
	/**
	 * Name of the id field
	 */
	public static final String ID_FIELD = "_id";

	/**
	 * Name of the doc id field (reference to the origin document)
	 */
	public static final String REFID_FIELD = "_docId";
	
	/**
	 * Extension for auditing tables 
	 */
	public static final String AUDITING_EXTENSION = "_history";
	
	/**
	 *
	 * @param dbo the original json object
	 * @param historyCollection the collection where the historical documents are stored
	 * @return the json object with increased version if <code>historyCollection != null</code> otherwise the original dbo
	 */
	protected static DBObject copyToHistoryCollection(DBObject dbo, DBCollection historyCollection) {
		if (historyCollection != null) {
			// clone the dbo
			BasicDBObject clone = new BasicDBObject();
			clone.putAll(dbo);
			Auditing.renameIdField(clone);
			Auditing.increaseVersion(clone);
			historyCollection.insert(clone);
			
			DBObject result = dbo;
			if (result instanceof LazyBSONObject) {
				// materialize lazy bson
				BasicDBObject expanded = new BasicDBObject();
				expanded.putAll(result);
				result = expanded;
			}
			Auditing.increaseVersion(result);
			return result;
		}
		return dbo;
	}

	/**
	 * Increases the version of a given versionable object
	 *
	 * @param pojo an object implementing the {@link IWithVersion} interface
	 */
	protected static void increaseVersion(IWithVersion pojo) {
		if (pojo.getDocumentVersion() == null) {
			pojo.setDocumentVersion(1);
		} else {
			pojo.setDocumentVersion(pojo.getDocumentVersion() + 1);
		}
	}
	
	/**
	 * Increases the version in a json document object
	 *
	 * @param dbo the json object to modify (Must NOT be a lazy initialized document (like {@link LazyBSONObject})
	 */
	private static void increaseVersion(DBObject dbo) {
		if (dbo.containsField(VERSION_FIELD)) {
			dbo.put(VERSION_FIELD, Integer.parseInt(String.valueOf(dbo.get(VERSION_FIELD))) + 1);
		} else {
			dbo.put(VERSION_FIELD, 1);
		}
		dbo.put(LASTCHANGE_FIELD, new Date());
	}

	/**
	 * Changes the _id field to {@link Auditing.REFID_FIELD} (to have a reference to the original document)
	 *
	 * @param dbo the json object
	 */
	private static void renameIdField(DBObject dbo) {
		if (dbo.containsField(ID_FIELD)) {
			Object id = dbo.get(ID_FIELD);
			dbo.put(REFID_FIELD, id);
			dbo.removeField(ID_FIELD);
		}
	}
}
