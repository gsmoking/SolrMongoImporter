package org.apache.solr.handler.dataimport;

import java.util.Map;

import org.bson.types.ObjectId;

/**
 * Created by Eclipse. User: Jhonson Date: 01/12/17 Time: 11:52 To change
 * this template use File | Settings | File Templates.
 * modify ObjectId transform Exception
 */
public class MongoMapperTransformer extends Transformer {

	@Override
	public Object transformRow(Map<String, Object> row, Context context) {
	
		for (Map<String, String> map : context.getAllEntityFields()) {
			String mongoFieldName = map.get(MONGO_FIELD);
			if (mongoFieldName == null)
				continue;
			String column = map.get(DataImporter.COLUMN);
			Object val = row.get(mongoFieldName);
			if("_id".equals(mongoFieldName)) {
				row.put(column, ((ObjectId)val).toString());
			} else {
				row.put(column, val);
			}
			
		}

		return row;
	}


	public static final String MONGO_FIELD = "mongoField";

}
