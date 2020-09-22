package com.reactiveminds.psi.common.kafka.pool;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class TestMain {
    public static void main(String[] args) {
        UUID u = UUID.nameUUIDFromBytes("sutanu".getBytes(StandardCharsets.UTF_8));
        System.out.println("  " + "sutanu".hashCode());
        String s = new String("sutanu");
        u = UUID.nameUUIDFromBytes("sutanu".getBytes(StandardCharsets.UTF_8));
        System.out.println("  " + s.hashCode());
        u = UUID.nameUUIDFromBytes("--0".getBytes(StandardCharsets.UTF_8));
        //System.out.println("  " + u.getMostSignificantBits());
    }
}
