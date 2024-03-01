package com.example.kafka;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BurgerMessage {

    private static final List<String> burgerShop = List.of("A001", "B001", "C001", "D001");
    private static final List<String> burgerMenu = List.of("Potato", "Beef", "Double Beef", "Hello Beef");

    public static void main(String[] args) {
        BurgerMessage burgerMessage = new BurgerMessage();

        long seed = 123L;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);
        for (int i = 0; i < 100; i++) {
            Map<String, String> burgerMap = burgerMessage.getMessage(faker, random, i);
            System.out.println("key: " + burgerMap.get("key") + " message: " + burgerMap.get("message"));
        }
    }

    public Map<String, String> getMessage(Faker faker, Random random, int id) {
        String shopId = getRandomValueFromList(burgerShop, random);
        String burgerName = getRandomValueFromList(burgerMenu, random);

        String orderId = "ord" + id;
        String customerName = faker.name().fullName();
        String phoneNumber = faker.phoneNumber().phoneNumber();
        String address = faker.address().fullAddress();
        LocalDateTime now = LocalDateTime.now();

        String record = String.format("order_id:%s, shop:%s, burgerName:%s, customer:%s, phoneNumber:%s, address:%s, time:%s",
                orderId, shopId, burgerName, customerName, phoneNumber, address, now);

        Map<String, String> messageMap = new HashMap<>();
        messageMap.put("key", shopId);
        messageMap.put("message", record);

        return messageMap;
    }

    private String getRandomValueFromList(List<String> tempList, Random random) {
        int size = tempList.size();
        int index = random.nextInt(size);

        return tempList.get(index);
    }
}
