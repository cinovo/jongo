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

import org.jongo.marshall.Unmarshaller;
import org.jongo.query.Query;
import org.jongo.query.QueryFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class FindAndModify {
	
	private final DBCollection collection;
	private final Unmarshaller unmarshaller;
	private final QueryFactory queryFactory;
	private final Query query;
	private Query fields, sort, modifier;
	private boolean remove = false;
	private boolean returnNew = false;
	private boolean upsert = false;
	private DBCollection historyCollection;
	
	
	FindAndModify(DBCollection collection, DBCollection historyCollection, Unmarshaller unmarshaller, QueryFactory queryFactory, String query, Object... parameters) {
		this.historyCollection = historyCollection;
		this.unmarshaller = unmarshaller;
		this.collection = collection;
		this.queryFactory = queryFactory;
		this.query = this.queryFactory.createQuery(query, parameters);
	}
	
	public FindAndModify with(String modifier, Object... parameters) {
		if (modifier == null) {
			throw new IllegalArgumentException("Modifier may not be null");
		}
		this.modifier = this.queryFactory.createQuery(modifier, parameters);
		return this;
	}
	
	public <T> T as(final Class<T> clazz) {
		return this.map(ResultHandlerFactory.newResultHandler(clazz, this.unmarshaller));
	}
	
	public <T> T map(ResultHandler<T> resultHandler) {
		
		DBObject dbObject = this.query.toDBObject();

		// first modify elements
		DBObject result = this.collection.findAndModify(dbObject, this.getAsDBObject(this.fields), this.getAsDBObject(this.sort), this.remove, this.getAsDBObject(this.modifier), this.returnNew, this.upsert);

		if (this.historyCollection != null) {
			// copy elements to history collection
			DBCursor find = this.collection.find(dbObject);
			while (find.hasNext()) {
				DBObject object = find.next();
				Auditing.copyToHistoryCollection(object, this.historyCollection);
			}

			// increase version stamps
			DBObject incModifier = this.queryFactory.createQuery("{ $inc: {" + Auditing.VERSION_FIELD + ": 1 }}").toDBObject();
			this.collection.update(dbObject, incModifier);
		}
		return result == null ? null : resultHandler.map(result);
	}
	
	public FindAndModify projection(String fields) {
		this.fields = this.queryFactory.createQuery(fields);
		return this;
	}
	
	public FindAndModify projection(String fields, Object... parameters) {
		this.fields = this.queryFactory.createQuery(fields, parameters);
		return this;
	}
	
	public FindAndModify sort(String sort) {
		this.sort = this.queryFactory.createQuery(sort);
		return this;
	}
	
	public FindAndModify remove() {
		this.remove = true;
		return this;
	}
	
	public FindAndModify returnNew() {
		this.returnNew = true;
		return this;
	}
	
	public FindAndModify upsert() {
		this.upsert = true;
		return this;
	}

	private DBObject getAsDBObject(Query query) {
		return query == null ? null : query.toDBObject();
	}
}
