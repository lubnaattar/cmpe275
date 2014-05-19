/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.resources;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


//import poke.dns.Users;
//import poke.dns.Courses;
import poke.server.resources.Resource;
//import poke.server.storage.MongoDataAccessObject;
import eye.Comm.Header;
import eye.Comm.Header.Routing;
import eye.Comm.Management;
import eye.Comm.NameSpace;
import eye.Comm.NameSpaceOperation;
import eye.Comm.NameSpaceStatus;
import eye.Comm.Payload;
import eye.Comm.PokeStatus;
import eye.Comm.Request;
import eye.Comm.NameSpaceOperation.SpaceAction;
import eye.Comm.Request.Builder;

public class NameSpaceResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	@Override
	public Request process(Request request) {
		return request;
		
	
	}

	@Override
	public Management processMgmtRequest(Request request) {
		// TODO Auto-generated method stub
		return null;
	}
}
