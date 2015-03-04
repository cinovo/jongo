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

import java.util.ArrayList;
import java.util.List;

import org.bson.LazyBSONCallback;
import org.bson.types.ObjectId;
import org.jongo.bson.Bson;
import org.jongo.bson.BsonDocument;
import org.jongo.marshall.Marshaller;
import org.jongo.query.QueryFactory;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.LazyDBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

class Insert {
	
	private final Marshaller marshaller;
	private final DBCollection collection;
	private final ObjectIdUpdater objectIdUpdater;
	private final QueryFactory queryFactory;
	private WriteConcern writeConcern;
	private DBCollection historyCollection;
	
	
	Insert(DBCollection collection, DBCollection historyCollection, WriteConcern writeConcern, Marshaller marshaller, ObjectIdUpdater objectIdUpdater, QueryFactory queryFactory) {
		this.historyCollection = historyCollection;
		this.writeConcern = writeConcern;
		this.marshaller = marshaller;
		this.collection = collection;
		this.objectIdUpdater = objectIdUpdater;
		this.queryFactory = queryFactory;
	}
	
	public WriteResult save(Object pojo) {
		Object id = this.preparePojo(pojo);
		DBObject dbo = Auditing.copyToHistoryCollection(this.convertToDBObject(pojo, id), this.historyCollection);
		if (this.historyCollection != null) {
			if (pojo instanceof IWithVersion) {
				Auditing.increaseVersion((IWithVersion) pojo);
			}
		}
		return this.collection.save(dbo, this.writeConcern);
	}

	public WriteResult insert(Object... pojos) {
		List<DBObject> dbos = new ArrayList<DBObject>(pojos.length);
		for (Object pojo : pojos) {
			Object id = this.preparePojo(pojo);
			DBObject dbo = this.convertToDBObject(pojo, id);
			dbos.add(Auditing.copyToHistoryCollection(dbo, this.historyCollection));
			if (this.historyCollection != null) {
				if (pojo instanceof IWithVersion) {
					Auditing.increaseVersion((IWithVersion) pojo);
				}
			}
		}
		return this.collection.insert(dbos, this.writeConcern);
	}
	
	public WriteResult insert(String query, Object... parameters) {
		DBObject dbo = this.queryFactory.createQuery(query, parameters).toDBObject();
		if (dbo instanceof BasicDBList) {
			return this.insert(((BasicDBList) dbo).toArray());
		}

		dbo = Auditing.copyToHistoryCollection(dbo, this.historyCollection);

		return this.collection.insert(dbo, this.writeConcern);
	}

	private Object preparePojo(Object pojo) {
		if (this.objectIdUpdater.mustGenerateObjectId(pojo)) {
			ObjectId newOid = ObjectId.get();
			this.objectIdUpdater.setObjectId(pojo, newOid);
			return newOid;
		}
		return this.objectIdUpdater.getId(pojo);
	}
	
	private DBObject convertToDBObject(Object pojo, Object id) {
		BsonDocument document = Insert.asBsonDocument(this.marshaller, pojo);
		return new LazyIdDBObject(document.toByteArray(), this.marshaller, id);
	}
	
	
	private final static class LazyIdDBObject extends LazyDBObject {
		
		private Object bsonId;
		private final Marshaller marshaller;
		
		
		private LazyIdDBObject(byte[] data, Marshaller marshaller, Object _id) {
			super(data, new LazyBSONCallback());
			this.marshaller = marshaller;
			this.bsonId = this.asBsonId(_id);
		}
		
		private Object asBsonId(Object _id) {
			if ((_id == null) || Bson.isPrimitive(_id)) {
				return _id;
			}
			return Insert.asBsonDocument(this.marshaller, _id).toDBObject();
		}
		
		@Override
		public Object put(String key, Object v) {
			if ("_id".equals(key)) {
				this.bsonId = this.asBsonId(key);
				return null; // fixme
			}
			throw new UnsupportedOperationException("Object is read only for fields others than _id");
		}
		
		@Override
		public Object get(String key) {
			if ("_id".equals(key) && (this.bsonId != null)) {
				return this.bsonId;
			}
			return super.get(key);
		}
	}


	private static BsonDocument asBsonDocument(Marshaller marshaller, Object obj) {
		try {
			return marshaller.marshall(obj);
		} catch (Exception e) {
			String message = String.format("Unable to save object %s due to a marshalling error", obj);
			throw new IllegalArgumentException(message, e);
		}
	}
}
