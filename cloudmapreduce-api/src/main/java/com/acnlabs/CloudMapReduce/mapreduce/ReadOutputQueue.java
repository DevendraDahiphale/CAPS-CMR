package com.acnlabs.CloudMapReduce.mapreduce;

import com.acnlabs.CloudMapReduce.DbManager;
import com.acnlabs.CloudMapReduce.QueueManager;
import com.acnlabs.CloudMapReduce.SimpleQueue;
import java.util.List;
import java.util.Map.Entry;

public class ReadOutputQueue extends MapReduce{

	private SimpleQueue outputQueue;
	public ReadOutputQueue(SimpleQueue outputQueue) {
		super(outputQueue);
	}
	public void read()
	{
		System.out.println("Receiving messages from:" + outputQueue);
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            for (Entry<String, String> entry : message.getAttributes().entrySet()) {
                System.out.println("  Attribute");
                System.out.println("    Name:  " + entry.getKey());
                System.out.println("    Value: " + entry.getValue());
            }
        }
        System.out.println();
	}
}
