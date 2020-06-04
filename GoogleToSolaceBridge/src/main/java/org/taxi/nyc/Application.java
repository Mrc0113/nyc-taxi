
package org.taxi.nyc;

import java.util.Date;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.JmsException;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableScheduling
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	@Autowired
	private JmsTemplate jmsTemplate = null;
	
	private ObjectMapper mapper = new ObjectMapper();
	private String topicPrefix = "taxi/nyc/v1/";
	private long counter=0;
	private Date startTime;

	public static void main(String[] args) {
		SpringApplication.run(Application.class);
	}

	@Bean
	public Consumer<TaxiStatusUpdatePayload> bridgeGcpToSolace() {
		return input -> {
			Double latitude = input.getLatitude();
			Double longitude = input.getLongitude();
			Double meterIncrement = input.getMeterIncrement();
			Double meterReading = input.getMeterReading();
			Integer passengerCount = input.getPassengerCount();
			Integer pointIdx = input.getPointIdx();
			String rideId = input.getRideId();
			String rideStatus = input.getRideStatus();
			String timestamp = input.getTimestamp();

			String topic = topicPrefix + rideStatus + "/" + passengerCount + "/" + rideId + "/" + longitude + "/"
					+ latitude;
			try {
				jmsTemplate.convertAndSend(topic, mapper.writeValueAsString(input));
				if(logger.isDebugEnabled()) {
					logger.debug("Sent " + input + " to topic:" + topic);
				}
       			counter++;
			} catch (JmsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		};
	}

	@PostConstruct
	private void configureJMSTemplate() {
		// Code that makes the JMS Template Cache Connections for Performance.
		CachingConnectionFactory ccf = new CachingConnectionFactory();
		ConnectionFactory cf = jmsTemplate.getConnectionFactory();
		ccf.setTargetConnectionFactory(cf);
		jmsTemplate.setConnectionFactory(ccf);
		jmsTemplate.setExplicitQosEnabled(true);
		jmsTemplate.setPubSubDomain(true);
		jmsTemplate.setTimeToLive(24 * 60 * 60 * 1000);
		jmsTemplate.setDeliveryPersistent(false);
		jmsTemplate.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		jmsTemplate.setPriority(0);
	}

	@Scheduled(fixedDelay=10000)
	public void printCount() {
        logger.info("Message Sent Count: "+ counter);
        Date currentTime = new Date();
        long seconds = ((currentTime.getTime() - startTime.getTime()) / 1000 );
        logger.info("Average Messages Per Second since Start: "+ Math.floorDiv(counter, seconds));
	}
	
	@PostConstruct
	private void setStartTime() {
		startTime = new Date();
	}
}
