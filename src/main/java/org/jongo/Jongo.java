/*
 * Copyright (C) 2011 Benoit GUEROUT <bguerout at gmail dot com> and Yves AMSELLEM <amsellem dot yves at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jongo;

import java.util.Date;

import org.bson.LazyBSONObject;
import org.jongo.bson.BsonDBDecoder;
import org.jongo.bson.BsonDBEncoder;
import org.jongo.marshall.jackson.JacksonMapper;
import org.jongo.query.Query;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class Jongo {
	
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

	private final DB database;
	private final Mapper mapper;

	private final boolean audited;


	public Jongo(DB database, boolean audited) {
		this(database, new JacksonMapper.Builder().build(), audited);
	}

	public Jongo(DB database, Mapper mapper, boolean audited) {
		this.database = database;
		this.mapper = mapper;
		this.audited = audited;
	}

	public MongoCollection getCollection(String name) {
		DBCollection dbCollection = this.database.getCollection(name);
		dbCollection.setDBDecoderFactory(BsonDBDecoder.FACTORY);
		dbCollection.setDBEncoderFactory(BsonDBEncoder.FACTORY);

		DBCollection dbHistoryCollection = null;
		if (this.audited) {
			dbHistoryCollection = this.database.getCollection(name + "_history");
			dbHistoryCollection.setDBDecoderFactory(BsonDBDecoder.FACTORY);
			dbHistoryCollection.setDBEncoderFactory(BsonDBEncoder.FACTORY);
		}
		return new MongoCollection(dbCollection, dbHistoryCollection, this.mapper);
	}

	public DB getDatabase() {
		return this.database;
	}

	public Mapper getMapper() {
		return this.mapper;
	}

	public Query createQuery(String query, Object... parameters) {
		return this.mapper.getQueryFactory().createQuery(query, parameters);
	}

	public Command runCommand(String query) {
		return this.runCommand(query, new Object[0]);
	}

	public Command runCommand(String query, Object... parameters) {
		return new Command(this.database, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), query, parameters);
	}
	
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
			Jongo.renameIdField(clone);
			Jongo.increaseVersion(clone);
			historyCollection.insert(clone);
			
			DBObject result = dbo;
			if (result instanceof LazyBSONObject) {
				// materialize lazy bson
				BasicDBObject expanded = new BasicDBObject();
				expanded.putAll(result);
				result = expanded;
			}
			Jongo.increaseVersion(result);
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
		if (dbo.containsField(Jongo.VERSION_FIELD)) {
			dbo.put(Jongo.VERSION_FIELD, Integer.parseInt(String.valueOf(dbo.get(Jongo.VERSION_FIELD))) + 1);
		} else {
			dbo.put(Jongo.VERSION_FIELD, 1);
		}
		dbo.put(Jongo.LASTCHANGE_FIELD, new Date());
	}

	/**
	 * Changes the _id field to {@link Jongo#REFID_FIELD} (to have a reference to the original document)
	 *
	 * @param dbo the json object
	 */
	private static void renameIdField(DBObject dbo) {
		if (dbo.containsField(Jongo.ID_FIELD)) {
			Object id = dbo.get(Jongo.ID_FIELD);
			dbo.put(Jongo.REFID_FIELD, id);
			dbo.removeField(Jongo.ID_FIELD);
		}
	}

}
