package pubsub.dataflow;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface PubSubToGCSOptions extends PipelineOptions, StreamingOptions {


    @Description("The Cloud Pub/Sub topic to read from.")
    @Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);

    @Description("Path of the output file including its filename prefix.")
    @Required
    String getOutput();

    void setOutput(String value);
	
}
