
package org.taxi.nyc;


import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class);
	}

	@Bean
	public Consumer<TaxiStatusUpdatePayload> bridgeGcpToSolace(StreamBridge streamBridge) {
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
		    
			String topic = "taxi/nyc/v1/"+rideStatus+"/"+passengerCount+"/"+rideId+"/"+longitude+"/"+latitude;  
			streamBridge.send(topic, input);
		};
	}

}
