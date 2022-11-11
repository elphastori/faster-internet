/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.elphastori.faster.ping;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.util.Collector;

import com.amazonaws.samples.connectors.timestream.TimestreamSink;
import com.amazonaws.samples.connectors.timestream.TimestreamSinkConfig;
import com.elphastori.faster.ping.kinesis.RoundRobinKinesisShardAssigner;
import com.elphastori.faster.ping.model.Ping;
import com.elphastori.faster.ping.model.TimestreamRecordConverter;
import com.elphastori.faster.ping.model.TimestreamRecordDeserializer;
import com.elphastori.faster.ping.utils.ParameterToolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Ping Flink Streaming Job.
 *
 * <p>To package the application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	// Currently Timestream supports max. 100 records in single write request. Do not increase this value.
	private static final int MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST = 100;
	private static final int MAX_CONCURRENT_WRITES_TO_TIMESTREAM = 1000;

	private static final String DEFAULT_STREAM_NAME = "PingStream";
	private static final String DEFAULT_REGION_NAME = "us-east-1";

	public static DataStream<Ping> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

		//set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
				parameter.get("Region", DEFAULT_REGION_NAME));
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

		if(adaptiveReadSettingStr.equals("true")) {
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
		} else {
			//poll new events from the Kinesis stream once every second
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
					parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
			// max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		FlinkKinesisConsumer<Ping> flinkKinesisConsumer = new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with EventSchema
				new TimestreamRecordDeserializer(),
				//using the previously defined properties
				kinesisConsumerConfig
		);
		flinkKinesisConsumer.setShardAssigner(new RoundRobinKinesisShardAssigner());

		return env
				.addSource(flinkKinesisConsumer)
				.name("KinesisSource");
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Ping> mappedInput = createKinesisSource(env, parameter);

		String region = parameter.get("Region", "us-east-1");
		String databaseName = parameter.get("TimestreamDbName", "faster");
		String tableName = parameter.get("TimestreamTableName", "pings");
		long memoryStoreTTLHours = Long.parseLong(parameter.get("MemoryStoreTTLHours", "168")); // 24 * 7
		long magneticStoreTTLDays = Long.parseLong(parameter.get("MagneticStoreTTLDays", "365"));

		// EndpointOverride is optional. Learn more here: https://docs.aws.amazon.com/timestream/latest/developerguide/architecture.html#cells
		String endpointOverride = parameter.get("EndpointOverride", "");
		if (endpointOverride.isEmpty()) {
			endpointOverride = null;
		}

		TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region, endpointOverride);
		timestreamInitializer.createDatabase(databaseName);
		timestreamInitializer.createTable(databaseName, tableName, memoryStoreTTLHours, magneticStoreTTLDays);

		TimestreamSink<Ping> sink = new TimestreamSink<>(
				(recordObject, context) -> TimestreamRecordConverter.convert(recordObject),
				(List<Record> records) -> {
					LOG.debug("Preparing WriteRecordsRequest with {} records", records.size());
					return WriteRecordsRequest.builder()
							.databaseName(databaseName)
							.tableName(tableName)
							.records(records)
							.build();
				},
				TimestreamSinkConfig.builder()
						.maxBatchSize(MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
						.maxBufferedRequests(100 * MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
						.maxInFlightRequests(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
						.maxTimeInBufferMS(15000)
						.emitSinkMetricsToCloudWatch(true)
						.writeClientConfig(TimestreamSinkConfig.WriteClientConfig.builder()
								.maxConcurrency(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
								.maxErrorRetry(10)
								.region(region)
								.requestTimeout(Duration.ofSeconds(20))
								.endpointOverride(endpointOverride)
								.build())
						.failureHandlerConfig(TimestreamSinkConfig.FailureHandlerConfig.builder()
								.failProcessingOnErrorDefault(true)
								.failProcessingOnRejectedRecordsException(true)
								.printFailedRequests(true)
								.build())
						.build()
		);

		mappedInput
				.keyBy((Ping ping) -> ping.getHost())
				.process(new TimeoutFunction(60 * 1000, 15 * 1000))
				.sinkTo(sink)
				.name("TimestreamSink")
				.disableChaining();

		env.execute("Ping Flink Streaming Job");
	}

	public static void pubTextSMS(SnsClient snsClient, String message, String phoneNumber) {
		try {
			PublishRequest request = PublishRequest.builder()
					.message(message)
					.phoneNumber(phoneNumber)
					.build();

			PublishResponse result = snsClient.publish(request);
			System.out.println(result.messageId() + " Message sent. Status was " + result.sdkHttpResponse().statusCode());
		} catch (SnsException e) {
			System.err.println(e.awsErrorDetails().errorMessage());
		}
	}

	public static class TimeoutFunction extends KeyedProcessFunction<String, Ping, Ping> {

		private final long timeout;
		private final long allowedLateness;
		private ValueState<Long> lastTimer;

		public TimeoutFunction(long timeout, long allowedLateness) {
			this.timeout = timeout;
			this.allowedLateness = allowedLateness;
		}

		@Override
		public void open(Configuration conf) {
			ValueStateDescriptor<Long> lastTimerDesc = new ValueStateDescriptor<>("lastTimer", Long.class);
			lastTimer = getRuntimeContext().getState(lastTimerDesc);
		}

		@Override
		public void processElement(Ping ping, Context context, Collector<Ping> out) throws IOException {
			long currentTime = context.timerService().currentProcessingTime();
			long timeoutTime = currentTime + timeout + allowedLateness;
			context.timerService().registerProcessingTimeTimer(timeoutTime);
			lastTimer.update(timeoutTime);
			out.collect(ping);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<Ping> out) throws IOException {
			if (timestamp == lastTimer.value()) {
				long timeoutTime = timestamp + timeout;
				context.timerService().registerProcessingTimeTimer(timeoutTime);
				lastTimer.update(timeoutTime);

				String host = context.getCurrentKey();

				ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();

				DynamoDbClient ddb = DynamoDbClient.builder()
						.credentialsProvider(credentialsProvider)
						.region(Region.US_EAST_1)
						.build();

				GetItemRequest request = GetItemRequest.builder()
						.key(Map.of("userName", AttributeValue.builder().s(host).build()))
						.tableName("FasterInternetUsers")
						.build();

				Map<String, AttributeValue> returnedItem;

				try {
					returnedItem = ddb.getItem(request).item();
					ddb.close();
				} catch (DynamoDbException e) {
					System.err.println(e.getMessage());
					return;
				}

				if (returnedItem == null) {
					System.out.format("No item found with the key %s!\n", host);
					return;
				}

				String phoneNumber = returnedItem.get("phoneNumber").s();

				String message = "Your internet connection is down. Please check your router.";
				SnsClient snsClient = SnsClient.builder()
								.credentialsProvider(credentialsProvider)
								.region(Region.US_EAST_1)
								.build();
				pubTextSMS(snsClient, message, phoneNumber);
				snsClient.close();

				out.collect(Ping.builder()
						.time(Instant.ofEpochMilli(timestamp - allowedLateness).toString())
						.google(0.0)
						.facebook(0.0)
						.amazon(0.0)
						.host(host)
						.build());
			}
		}
	}
}