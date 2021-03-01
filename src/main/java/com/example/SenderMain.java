package com.example;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

public class SenderMain {

	private static final Properties properties;
	
    static {
        properties = new Properties();
        try {
            properties.load(Files.newBufferedReader(Paths.get("./sqs_client.properties")
                    , StandardCharsets.UTF_8));
        } catch (IOException e) {
            // ファイル読み込みに失敗
            System.out.println(String.format("ファイルの読み込みに失敗しました。ファイル名:%s", "sqs_client.properties"));
        }
    }

	public static void main(String[] args) {

		/*
         * Create a new instance of the builder with all defaults (credentials
         * and region) set automatically. For more information, see
         * Creating Service Clients in the AWS SDK for Java Developer Guide.
         */
		AmazonSQSClientBuilder builder = AmazonSQSClientBuilder.standard();
		builder.setCredentials(new InstanceProfileCredentialsProvider(false));
        final AmazonSQS sqs = builder.build();
        
        final String myQueueUrl = properties.getProperty("queue_url");
        System.out.println("queue_url="+myQueueUrl);
        
        int count = Integer.parseInt(args[0]);
        
        long start = System.nanoTime();
        
        long sum = 0;
        
        String base = Long.toString(System.currentTimeMillis());
        
		for (int i = 0; i < count; i++) {
			
			try {
				
				final SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl,
						"This is my message text " + i);

				/*
				 * When you send messages to a FIFO queue, you must provide a non-empty
				 * MessageGroupId.
				 */
				sendMessageRequest.setMessageGroupId("messageGroup1");

				// Uncomment the following to provide the MessageDeduplicationId
				sendMessageRequest.setMessageDeduplicationId(base+"_"+i);

				long p1 = System.nanoTime();
//				final SendMessageResult sendMessageResult = 
				sqs.sendMessage(sendMessageRequest);

				sum += (System.nanoTime() - p1);
				
//				final String sequenceNumber = sendMessageResult.getSequenceNumber();
//				final String messageId = sendMessageResult.getMessageId();

//				System.out.println("Index "+i+" SendMessage succeed with messageId " + messageId + ", sequence number "
//						+ sequenceNumber + "\n");

			} catch (final AmazonServiceException ase) {
				System.out.println("Caught an AmazonServiceException, which means "
						+ "your request made it to Amazon SQS, but was "
						+ "rejected with an error response for some reason.");
				System.out.println("Error Message:    " + ase.getMessage());
				System.out.println("HTTP Status Code: " + ase.getStatusCode());
				System.out.println("AWS Error Code:   " + ase.getErrorCode());
				System.out.println("Error Type:       " + ase.getErrorType());
				System.out.println("Request ID:       " + ase.getRequestId());
			} catch (final AmazonClientException ace) {
				System.out.println("Caught an AmazonClientException, which means "
						+ "the client encountered a serious internal problem while "
						+ "trying to communicate with Amazon SQS, such as not " + "being able to access the network.");
				System.out.println("Error Message: " + ace.getMessage());
			}
		}
		
		long diff = System.nanoTime() - start;
		
		System.out.println("Count : "+count);
		
		BigDecimal bd = new BigDecimal(1000);
		BigDecimal bdDiff = new BigDecimal(diff);
		BigDecimal sec = bdDiff.divide(bd).divide(bd).divide(bd);
		System.out.println("Total sec : "+sec.toPlainString());
		
		BigDecimal countBd = new BigDecimal(count);
		BigDecimal tps = countBd.divide(sec, 10, RoundingMode.HALF_UP);
		System.out.println("Tps : "+tps.toPlainString());
		
		BigDecimal sumBd = new BigDecimal(sum);
		BigDecimal spt = sumBd.divide(countBd).divide(bd).divide(bd).divide(bd);
		System.out.println("Sec per sendMessage : "+spt.toPlainString());
	}

}
