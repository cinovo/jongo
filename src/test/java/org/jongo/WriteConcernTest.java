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

import org.jongo.marshall.jackson.JacksonMapper;
import org.jongo.model.Friend;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class WriteConcernTest {
	
	DBCollection mockedDBCollection = Mockito.mock(DBCollection.class);
	DBCollection mockedHistoryDBCollection = Mockito.mock(DBCollection.class);
	private MongoCollection collection;
	
	
	@Before
	public void setUp() throws Exception {
		this.collection = new MongoCollection(this.mockedDBCollection, this.mockedHistoryDBCollection, new JacksonMapper.Builder().build());
	}
	
	@Test
	public void shouldUseDefaultDriverWriteConcern() throws Exception {
		
		Friend john = new Friend("John");
		
		this.collection.save(john);
		
		Mockito.verify(this.mockedDBCollection).save(Matchers.any(DBObject.class), Matchers.isNull(WriteConcern.class));
	}
	
	@Test
	public void canSaveWithCustomWriteConcernOnCollection() throws Exception {
		
		Friend john = new Friend("John");
		
		this.collection.withWriteConcern(WriteConcern.ACKNOWLEDGED).save(john);
		
		Mockito.verify(this.mockedDBCollection).save(Matchers.any(DBObject.class), Matchers.eq(WriteConcern.ACKNOWLEDGED));
	}
	
	@Test
	public void canInsertWithCustomWriteConcernOnCollection() throws Exception {
		
		this.collection.withWriteConcern(WriteConcern.SAFE).insert("{name : 'Abby'}");
		
		Mockito.verify(this.mockedDBCollection).insert(Matchers.any(DBObject.class), Matchers.eq(WriteConcern.SAFE));
	}
	
	@Test
	public void canUpdateWithCustomWriteConcernOnCollection() throws Exception {
		
		this.collection.withWriteConcern(WriteConcern.SAFE).update("{}").upsert().with("{$set:{name:'John'}}");
		
		Mockito.verify(this.mockedDBCollection).update(Matchers.any(DBObject.class), Matchers.any(DBObject.class), Matchers.eq(true), Matchers.eq(false), Matchers.eq(WriteConcern.SAFE));
	}
	
	@Test
	public void canRemoveWithCustomWriteConcernOnCollection() throws Exception {
		
		this.collection.withWriteConcern(WriteConcern.SAFE).remove();
		
		Mockito.verify(this.mockedDBCollection).remove(Matchers.any(DBObject.class), Matchers.eq(WriteConcern.SAFE));
	}
	
}
