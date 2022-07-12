package pubsub.dataflow;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.Write;
import com.google.protobuf.InvalidProtocolBufferException;

import pubsub.model.CustomMessage;

public class PubSubToGCS {

	public static void main(String[] args) throws IOException {
		final FirestoreOptions fireStoreOptions = FirestoreOptions.getDefaultInstance().toBuilder()
	            .setProjectId("peerless-glass-355409")
	            .setCredentials(GoogleCredentials.getApplicationDefault())
	            .build();
		
		int numShards = 1;
		
		PubSubToGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGCSOptions.class);
		
		options.setStreaming(Boolean.TRUE);
		
		Pipeline pipeline = Pipeline.create(options);
		
		CustomMessage message = new CustomMessage(null, false, "project");
		
		PCollection<String> output = pipeline
			.apply("Read PubSub Message from publisher : ", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
			.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));
		

		//output.apply("Write file to GCS : ", new WriteOneFilePerWindow(options.getOutput(), numShards));
		//output.apply(DatastoreIO.v1().write().withProjectId("hl"));
		
		output.apply("Write file to GCS : ", new WriteOneFilePerWindow(options.getOutput(), numShards));
		
		output.apply(ParDo.of(new DoFn<String, Write>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			
			@ProcessElement
			public void processElement(@Element String data, OutputReceiver<Write> out) throws InvalidProtocolBufferException, IllegalArgumentException, IllegalAccessException {
				message.setData(data);
				Write write = Write.newBuilder()
						.setUpdate(Document.newBuilder()
								.setName(createDocName(fireStoreOptions, "doc-collection", "doc-1"))
								.putAllFields(gnerateMapFromModel(message))
								.build())
						.build();
				out.output(write);
			}

			
			
		})).apply(FirestoreIO.v1().write().batchWrite().build());
		
		 pipeline.run().waitUntilFinish();
			
	}
	
	protected static String createDocName(FirestoreOptions fireStoreOptions, String collectionId, String docId) {
		String docPath = 
				String.format(
						"projects/%s/database/%s/documents", fireStoreOptions.getProjectId(), fireStoreOptions.getDatabaseId());
				
		return docPath+"/"+collectionId+"/"+docId;
	}

	private static Map<String, Value> gnerateMapFromModel(CustomMessage message) throws InvalidProtocolBufferException, IllegalArgumentException, IllegalAccessException {
		Map<String, Value> values = new HashMap<String, Value>();
		
		Field[] fields = message.getClass().getDeclaredFields();
		for(Field field : fields) {
			values.put(field.getName(), Value.parseFrom(String.valueOf(field.get(message)).getBytes()));
		}
		
		return values;
	}
}
