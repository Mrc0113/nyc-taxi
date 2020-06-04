
package org.taxi.nyc;

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
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	@Autowired
	private JmsTemplate jmsTemplate = null;

	@Autowired
	private JmsTemplate jmsTemplate2 = null;
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
				logger.info("Sending " + input + " to topic:" + topic);
			} catch (JmsException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		};
	}

	@Bean
	public Consumer<TaxiStatusUpdatePayload> bridgeGcpToSolace2() {
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
				jmsTemplate2.convertAndSend(topic, mapper.writeValueAsString(input));
//				logger.info("Sending "+input+" to topic:"+topic);
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
	

	@PostConstruct
	private void fixJMSTemplate2() {
		// Code that makes the JMS Template Cache Connections for Performance.
		CachingConnectionFactory ccf = new CachingConnectionFactory();
		ccf.setTargetConnectionFactory(jmsTemplate2.getConnectionFactory());
		jmsTemplate2.setConnectionFactory(ccf);
		jmsTemplate2.setExplicitQosEnabled(true);
		jmsTemplate2.setPubSubDomain(true);
		jmsTemplate2.setTimeToLive(24 * 60 * 60 * 1000);
		jmsTemplate2.setDeliveryPersistent(false);
		jmsTemplate2.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		jmsTemplate2.setPriority(0);
	}
}
