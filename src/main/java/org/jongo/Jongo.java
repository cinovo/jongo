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

import org.jongo.bson.BsonDBDecoder;
import org.jongo.bson.BsonDBEncoder;
import org.jongo.marshall.jackson.JacksonMapper;
import org.jongo.query.Query;

import com.mongodb.DB;
import com.mongodb.DBCollection;

public class Jongo {
	
	private final DB database;
	private final Mapper mapper;

	private final boolean audited;

	public Jongo(DB database) {
        this(database, new JacksonMapper.Builder().build());
    }

    public Jongo(DB database, Mapper mapper) {
        this.database = database;
        this.mapper = mapper;
        this.audited = false;
    }

    // new constructor to enable auditing
	public Jongo(DB database, boolean audited) {
		this(database, new JacksonMapper.Builder().build(), audited);
	}

    // new constructor to enable auditing
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
			dbHistoryCollection = this.database.getCollection(name + Auditing.AUDITING_EXTENSION);
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

}
