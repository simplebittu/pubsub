package pubsub.dataflow;

import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class PubSubToGCS {

	public static void main(String[] args) {
		
		int numShards = 1;
		
		PubSubToGCSOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToGCSOptions.class);
		
		options.setStreaming(Boolean.TRUE);
		
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<String> output = pipeline
			.apply("Read PubSub Message from publisher : ", PubsubIO.readStrings().fromTopic(options.getInputTopic()))
			.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));
		

		//output.apply("Write file to GCS : ", new WriteOneFilePerWindow(options.getOutput(), numShards));
		//output.apply(DatastoreIO.v1().write().withProjectId("hl"));
		
		output.apply("Write file to local : ", TextIO.write().to("./output.message.txt"));
		
		 pipeline.run().waitUntilFinish();
			
	}
}
