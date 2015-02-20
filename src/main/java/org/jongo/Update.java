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

import org.bson.LazyBSONObject;
import org.jongo.query.Query;
import org.jongo.query.QueryFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class Update {
	
	private final DBCollection collection;
	private final Query query;
	private final QueryFactory queryFactory;
	
	private WriteConcern writeConcern;
	private boolean upsert = false;
	private boolean multi = false;
	private DBCollection historyCollection;
	
	
	Update(DBCollection collection, DBCollection historyCollection, WriteConcern writeConcern, QueryFactory queryFactory, String query, Object... parameters) {
		this.collection = collection;
		this.historyCollection = historyCollection;
		this.writeConcern = writeConcern;
		this.queryFactory = queryFactory;
		this.query = this.createQuery(query, parameters);
	}
	
	public WriteResult with(String modifier) {
		return this.with(modifier, new Object[0]);
	}
	
	public WriteResult with(String modifier, Object... parameters) {
		Query updateQuery = this.queryFactory.createQuery(modifier, parameters);
		WriteResult writeResult = this.collection.update(this.query.toDBObject(), updateQuery.toDBObject(), this.upsert, this.multi, this.writeConcern);
		
		return writeResult;
	}
	
	public WriteResult with(Object pojo) {
		
		DBObject updateDbo = this.queryFactory.createQuery("{$set:#}", pojo).toDBObject();
		this.removeIdField(updateDbo);
		DBObject findQuery = this.query.toDBObject();
		WriteResult writeResult = this.collection.update(findQuery, updateDbo, this.upsert, this.multi, this.writeConcern);
		
		if (this.historyCollection != null) {
			// copy elements to history collection
			DBCursor find = this.collection.find(findQuery);
			while (find.hasNext()) {
				DBObject object = find.next();
				Jongo.copyToHistoryCollection(object, this.historyCollection);
			}

			// increase version stamps
			DBObject incModifier = this.queryFactory.createQuery("{$inc:{" + Jongo.VERSION_FIELD + ": 1}}").toDBObject();
			this.collection.update(findQuery, incModifier);
		}
		return writeResult;
	}
	
	private void removeIdField(DBObject updateDbo) {
		DBObject pojoAsDbo = (DBObject) updateDbo.get("$set");
		if (pojoAsDbo.containsField("_id")) {
			// Need to materialize lazy objects which are read only
			if (pojoAsDbo instanceof LazyBSONObject) {
				BasicDBObject expanded = new BasicDBObject();
				expanded.putAll(pojoAsDbo);
				updateDbo.put("$set", expanded);
				pojoAsDbo = expanded;
			}
			pojoAsDbo.removeField("_id");
		}
	}
	
	public Update upsert() {
		this.upsert = true;
		return this;
	}
	
	public Update multi() {
		this.multi = true;
		return this;
	}
	
	private Query createQuery(String query, Object[] parameters) {
		try {
			return this.queryFactory.createQuery(query, parameters);
		} catch (Exception e) {
			String message = String.format("Unable execute update operation using query %s", query);
			throw new IllegalArgumentException(message, e);
		}
	}
}
