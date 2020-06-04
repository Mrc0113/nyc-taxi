
package org.taxi.nyc;

import java.util.function.Consumer;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.JmsException;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	@Autowired
	private JmsTemplate jmsTemplate = null;
	private ObjectMapper mapper = new ObjectMapper();
	private String topicPrefix = "taxi/nyc/v1/";

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
				logger.info("Sending "+input+" to topic:"+topic);
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
	private void fixJMSTemplate() {
		// Code that makes the JMS Template Cache Connections for Performance.
		CachingConnectionFactory ccf = new CachingConnectionFactory();
		ccf.setTargetConnectionFactory(jmsTemplate.getConnectionFactory());
		jmsTemplate.setConnectionFactory(ccf);
		jmsTemplate.setPubSubDomain(true);
		jmsTemplate.setTimeToLive(24*60*60*1000);
	}

}
