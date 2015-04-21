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

import org.bson.types.ObjectId;
import org.jongo.query.Query;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class MongoCollection {

	public static final String MONGO_DOCUMENT_ID_NAME = "_id";
	public static final String MONGO_QUERY_OID = "$oid";
	private static final Object[] NO_PARAMETERS = {};
	private static final String ALL = "{}";

	private final DBCollection collection;
	private final WriteConcern writeConcern;
	private final ReadPreference readPreference;
	private final Mapper mapper;
	private DBCollection historyCollection;


	public MongoCollection(DBCollection dbCollection, DBCollection dbHistoryCollection, Mapper mapper) {
		this(dbCollection, dbHistoryCollection, mapper, dbCollection.getWriteConcern(), dbCollection.getReadPreference());
	}

	private MongoCollection(DBCollection dbCollection, DBCollection dbHistoryCollection, Mapper mapper, WriteConcern writeConcern, ReadPreference readPreference) {
		this.collection = dbCollection;
		this.historyCollection = dbHistoryCollection;
		this.writeConcern = writeConcern;
		this.readPreference = readPreference;
		this.mapper = mapper;
	}

	public MongoCollection withWriteConcern(WriteConcern concern) {
		return new MongoCollection(this.collection, this.historyCollection, this.mapper, concern, this.readPreference);
	}

	public MongoCollection withReadPreference(ReadPreference readPreference) {
		return new MongoCollection(this.collection, this.historyCollection, this.mapper, this.writeConcern, readPreference);
	}

	public FindOne findOne(ObjectId id) {
		if (id == null) {
			throw new IllegalArgumentException("Object id must not be null");
		}
		return new FindOne(this.collection, this.readPreference, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), "{_id:#}", id);
	}

	public FindOne findOne() {
		return this.findOne(MongoCollection.ALL);
	}

	public FindOne findOne(String query) {
		return this.findOne(query, MongoCollection.NO_PARAMETERS);
	}

	public FindOne findOne(String query, Object... parameters) {
		return new FindOne(this.collection, this.readPreference, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), query, parameters);
	}

	public Find find() {
		return this.find(MongoCollection.ALL);
	}

	public Find find(String query) {
		return this.find(query, MongoCollection.NO_PARAMETERS);
	}

	public Find find(String query, Object... parameters) {
		return new Find(this.collection, this.readPreference, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), query, parameters);
	}
	
	public Find findHistory(String query, Object... parameters) {
		return new Find(this.historyCollection, this.readPreference, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), query, parameters);
	}

	public FindAndModify findAndModify() {
		return this.findAndModify(MongoCollection.ALL);
	}

	public FindAndModify findAndModify(String query) {
		return this.findAndModify(query, MongoCollection.NO_PARAMETERS);
	}

	public FindAndModify findAndModify(String query, Object... parameters) {
		return new FindAndModify(this.collection, this.historyCollection, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), query, parameters);
	}

	public long count() {
		return this.collection.getCount(this.readPreference);
	}

	public long count(String query) {
		return this.count(query, MongoCollection.NO_PARAMETERS);
	}

	public long count(String query, Object... parameters) {
		DBObject dbQuery = this.createQuery(query, parameters).toDBObject();
		return this.collection.getCount(dbQuery, null, this.readPreference);
	}

	public Update update(String query) {
		return this.update(query, MongoCollection.NO_PARAMETERS);
	}

	public Update update(ObjectId id) {
		if (id == null) {
			throw new IllegalArgumentException("Object id must not be null");
		}
		return this.update("{_id:#}", id);
	}

	public Update update(String query, Object... parameters) {
		return new Update(this.collection, this.historyCollection, this.writeConcern, this.mapper.getQueryFactory(), query, parameters);
	}

	public WriteResult save(Object pojo) {
		return new Insert(this.collection, this.historyCollection, this.writeConcern, this.mapper.getMarshaller(), this.mapper.getObjectIdUpdater(), this.mapper.getQueryFactory()).save(pojo);
	}

	public WriteResult insert(Object pojo) {
		return this.insert(new Object[] {pojo});
	}

	public WriteResult insert(String query) {
		return this.insert(query, MongoCollection.NO_PARAMETERS);
	}

	public WriteResult insert(Object... pojos) {
		return new Insert(this.collection, this.historyCollection, this.writeConcern, this.mapper.getMarshaller(), this.mapper.getObjectIdUpdater(), this.mapper.getQueryFactory()).insert(pojos);
	}

	public WriteResult insert(String query, Object... parameters) {
		return new Insert(this.collection, this.historyCollection, this.writeConcern, this.mapper.getMarshaller(), this.mapper.getObjectIdUpdater(), this.mapper.getQueryFactory()).insert(query, parameters);
	}

	public WriteResult remove(ObjectId id) {
		return this.remove("{" + MongoCollection.MONGO_DOCUMENT_ID_NAME + ":#}", id);
	}

	public WriteResult remove() {
		return this.remove(MongoCollection.ALL);
	}

	public WriteResult remove(String query) {
		return this.remove(query, MongoCollection.NO_PARAMETERS);
	}

	public WriteResult remove(String query, Object... parameters) {
		return this.collection.remove(this.createQuery(query, parameters).toDBObject(), this.writeConcern);
	}

	public Distinct distinct(String key) {
		return new Distinct(this.collection, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory(), key);
	}

	public Aggregate aggregate(String pipelineOperator) {
		return this.aggregate(pipelineOperator, MongoCollection.NO_PARAMETERS);
	}

	public Aggregate aggregate(String pipelineOperator, Object... parameters) {
		return new Aggregate(this.collection, this.mapper.getUnmarshaller(), this.mapper.getQueryFactory()).and(pipelineOperator, parameters);
	}

	public void drop() {
		this.collection.drop();
	}

	public void dropIndex(String keys) {
		this.collection.dropIndex(this.createQuery(keys).toDBObject());
	}

	public void dropIndexes() {
		this.collection.dropIndexes();
	}

	public void ensureIndex(String keys) {
		this.collection.createIndex(this.createQuery(keys).toDBObject());
	}

	public void ensureIndex(String keys, String options) {
		this.collection.createIndex(this.createQuery(keys).toDBObject(), this.createQuery(options).toDBObject());
	}

	public String getName() {
		return this.collection.getName();
	}

	public DBCollection getDBCollection() {
		return this.collection;
	}

	private Query createQuery(String query, Object... parameters) {
		return this.mapper.getQueryFactory().createQuery(query, parameters);
	}

	@Override
	public String toString() {
		if (this.collection != null) {
			return "collection {" + "name: '" + this.collection.getName() + "', db: '" + this.collection.getDB().getName() + "'}";
		} else {
			return super.toString();
		}
	}
}
