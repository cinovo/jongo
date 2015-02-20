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

package org.jongo.util;

import java.net.UnknownHostException;

import org.jongo.Jongo;
import org.jongo.Mapper;
import org.jongo.MongoCollection;
import org.jongo.marshall.jackson.JacksonMapper;
import org.junit.Assume;
import org.junit.BeforeClass;

import com.mongodb.CommandResult;
import com.mongodb.DB;

public abstract class JongoTestCase {

	private static MongoResource mongoResource;

	private Jongo jongo;
	private Mapper mapper;


	public JongoTestCase() {
		this.mapper = new JacksonMapper.Builder().build();
		this.jongo = new Jongo(JongoTestCase.mongoResource.getDb("test_jongo"), this.mapper, false);
	}

	@BeforeClass
	public static void startMongo() throws Exception {
		JongoTestCase.mongoResource = new MongoResource();
	}

	protected MongoCollection createEmptyCollection(String collectionName) throws UnknownHostException {
		MongoCollection col = this.jongo.getCollection(collectionName);
		col.drop();
		return col;
	}

	protected void dropCollection(String collectionName) throws UnknownHostException {
		this.getDatabase().getCollection(collectionName).drop();
	}

	protected DB getDatabase() throws UnknownHostException {
		return this.jongo.getDatabase();
	}

	protected Jongo getJongo() {
		return this.jongo;
	}

	protected Mapper getMapper() {
		return this.mapper;
	}

	protected void assumeThatMongoVersionIsGreaterThan(String expectedVersion) throws UnknownHostException {
		int expectedVersionAsInt = Integer.valueOf(expectedVersion.replaceAll("\\.", ""));
		CommandResult buildInfo = this.getDatabase().command("buildInfo");
		String version = (String) buildInfo.get("version");
		int currentVersion = Integer.valueOf(version.replaceAll("\\.", ""));
		Assume.assumeTrue(currentVersion >= expectedVersionAsInt);
	}

	public void prepareMarshallingStrategy(Mapper mapper) {
		this.mapper = mapper;
		this.jongo = new Jongo(JongoTestCase.mongoResource.getDb("test_jongo"), mapper, false);
	}

}
