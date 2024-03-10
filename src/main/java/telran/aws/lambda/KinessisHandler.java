package telran.aws.lambda;

import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class KinessisHandler implements RequestHandler<KinesisEvent, String>{

	private static final String TABLE_NAME = "sensor_data";
	@Override
	public String handleRequest(KinesisEvent input, Context context) {
		var logger = context.getLogger();
		logger.log("handler started");
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient(); 
		DynamoDB dynamoDb = new DynamoDB(client); 
		Table table = dynamoDb.getTable(TABLE_NAME);
		try {
			List<Map<String, Object>> probes = input.getRecords().stream()
					.map(t -> {
						try {
							return toMap(t);
						} catch (ParseException e) {
							throw new RuntimeException(e);
						}
					}).toList();
			probes.forEach(p -> {
				logger.log(p.toString());
				table.putItem(new PutItemSpec().withItem(Item.fromMap(p)));
			});
			
			
			
		} catch (Exception e) {
			logger.log("error: " + e.toString());
		}
		
		return null;
	}
	@SuppressWarnings("unchecked")
	Map<String, Object> toMap(KinesisEventRecord record) throws ParseException {
		String str = new String(record.getKinesis().getData().array());
		int index = str.indexOf('{'); //start JSON data
		String jsonStr = str.substring(index);
		JSONParser parser = new JSONParser();
	
		Map<String, Object> mapJSON = (Map<String, Object>) parser.parse(jsonStr);
		return mapJSON;
	}
}
