package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * User: Johnson Date: 01/12/17 Time: 11:46 To change this template use File |
 * Settings | File Templates.
 * Mongodb 3.3.4
 */

public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

	private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);

	private MongoCollection<Document> mongoCollection;
	private MongoDatabase mongoDb;
	private MongoClient mongoConnection;

	private MongoCursor<Document> mongoCursor;

	@Override
	public void init(Context context, Properties initProps) {
		String databaseName = initProps.getProperty(DATABASE);
		String host = initProps.getProperty(HOST, "localhost");
		int port = Integer.parseInt(initProps.getProperty(PORT, "27017"));
		String username = initProps.getProperty(USERNAME);
		String password = initProps.getProperty(PASSWORD);
		if (databaseName == null) {
			throw new DataImportHandlerException(SEVERE, "Database must be supplied");
		}

		try {
			MongoCredential credential = MongoCredential.createCredential(username, databaseName,
					password.toCharArray());
			MongoClient mongo = new MongoClient(new ServerAddress(host, port),
                    Arrays.asList(credential));
			
			MongoDatabase database = mongo.getDatabase(databaseName);
			database.withReadPreference(ReadPreference.secondaryPreferred());
			this.mongoConnection = mongo;
			this.mongoDb = database;

		} catch (Exception e) {
			throw new DataImportHandlerException(SEVERE, "Unable to connect to Mongo");
		}
	}

	@Override
	public Iterator<Map<String, Object>> getData(String query) {

		Document queryObject = Document.parse(query);
		LOG.debug("Executing MongoQuery: " + query.toString());
		
		long start = System.currentTimeMillis();
		mongoCursor = this.mongoCollection.find(queryObject).iterator();
		LOG.trace("Time taken for mongo :" + (System.currentTimeMillis() - start));

		ResultSetIterator resultSet = new ResultSetIterator(mongoCursor);
		return resultSet.getIterator();
	}

	public Iterator<Map<String, Object>> getData(String query, String collection) {
		this.mongoCollection = this.mongoDb.getCollection(collection);
		return getData(query);
	}

	private class ResultSetIterator {
		MongoCursor<Document> MongoCursor;

		Iterator<Map<String, Object>> rSetIterator;

		public ResultSetIterator(MongoCursor<Document> MongoCursor) {
			this.MongoCursor = MongoCursor;

			rSetIterator = new Iterator<Map<String, Object>>() {
				public boolean hasNext() {
					return hasnext();
				}

				public Map<String, Object> next() {
					return getARow();
				}

				public void remove() {/* do nothing */
				}
			};

		}

		public Iterator<Map<String, Object>> getIterator() {
			return rSetIterator;
		}

		private Map<String, Object> getARow() {
			Document mongoObject = getMongoCursor().next();

			Map<String, Object> result = new HashMap<String, Object>();
			Set<String> keys = mongoObject.keySet();
			Iterator<String> iterator = keys.iterator();

			while (iterator.hasNext()) {
				String key = iterator.next();
				Object innerObject = mongoObject.get(key);

				result.put(key, innerObject);
			}

			return result;
		}

		private boolean hasnext() {
			if (MongoCursor == null)
				return false;
			try {
				if (MongoCursor.hasNext()) {
					return true;
				} else {
					close();
					return false;
				}
			} catch (MongoException e) {
				close();
				wrapAndThrow(SEVERE, e);
				return false;
			}
		}

		private void close() {
			try {
				if (MongoCursor != null)
					MongoCursor.close();
			} catch (Exception e) {
				LOG.warn("Exception while closing result set", e);
			} finally {
				MongoCursor = null;
			}
		}
	}

	private MongoCursor<Document> getMongoCursor() {
		return this.mongoCursor;
	}

	@Override
	public void close() {
		if (this.mongoCursor != null) {
			this.mongoCursor.close();
		}

		if (this.mongoConnection != null) {
			this.mongoConnection.close();
		}
	}

	public static final String DATABASE = "database";
	public static final String HOST = "host";
	public static final String PORT = "port";
	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";

}
