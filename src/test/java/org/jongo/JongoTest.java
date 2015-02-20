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

import org.assertj.core.api.Assertions;
import org.jongo.model.Friend;
import org.jongo.query.Query;
import org.jongo.util.JongoTestCase;
import org.junit.Test;

public class JongoTest extends JongoTestCase {
	
	@Test
	public void canObtainACollection() throws Exception {
		
		Jongo jongo = new Jongo(this.getDatabase(), false);
		
		MongoCollection collection = jongo.getCollection("collection-name");
		
		Assertions.assertThat(collection).isNotNull();
		Assertions.assertThat(collection.getName()).isEqualTo("collection-name");
	}
	
	@Test
	public void canCreateQuery() throws Exception {
		
		Jongo jongo = new Jongo(this.getDatabase(), false);
		
		Query query = jongo.createQuery("{test:1}");
		
		Assertions.assertThat(query.toDBObject().get("test")).isEqualTo(1);
	}
	
	@Test
	public void canGetMapper() throws Exception {
		
		Jongo jongo = new Jongo(this.getDatabase(), false);
		
		Mapper mapper = jongo.getMapper();
		
		Assertions.assertThat(mapper).isNotNull();
		Assertions.assertThat(mapper.getMarshaller().marshall(new Friend("test"))).isNotNull();
	}
}
