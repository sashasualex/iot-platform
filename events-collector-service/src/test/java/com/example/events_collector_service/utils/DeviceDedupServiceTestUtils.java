package com.example.events_collector_service.utils;

import com.nashkod.avro.DeviceEvent;

public class DeviceDedupServiceTestUtils {

    public static String getRandomDeviceId() {
        return "device-" + System.currentTimeMillis();
    }

    public static String getDeviceId1() {
        return "device-1";
    }

    public static String getDeviceId2() {
        return "device-2";
    }

    public static String getDeviceId3() {
        return "device-3";
    }

    public static DeviceEvent getDeviceEvent() {
        return DeviceEvent.newBuilder()
                .setEventId("s-1")
                .setDeviceId("e-1")
                .setTimestamp(1700000000000L)
                .setType("TEMP")
                .setPayload("{\"t\":23}")
                .build();
    }

}
