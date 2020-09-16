package com.reactiveminds.psi.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        byte[] in = "0000a78d-3a9d-86fc-fe29-2cf21a882dac|individualPreferenceSelectionResponseMsg|0000a78d-3a9d-86fc-fe29-2cf21a882dac".getBytes(StandardCharsets.UTF_8);
        byte [] out = null;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final GZIPOutputStream gzipOutput = new GZIPOutputStream(baos)) {
            gzipOutput.write(in);
            gzipOutput.finish();
            out = baos.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Error while compression!", e);
        }


        System.out.println("in len: "+in.length+", out len: "+out.length);

        System.out.println(UUID.nameUUIDFromBytes("100000".getBytes(StandardCharsets.UTF_8)));
        Thread.sleep(2000);
        System.out.println(UUID.nameUUIDFromBytes("SUTANU".getBytes(StandardCharsets.UTF_8)));
        // e9b0409b-2a91-329b-bdb2-194d4e1859d7
    }
}
