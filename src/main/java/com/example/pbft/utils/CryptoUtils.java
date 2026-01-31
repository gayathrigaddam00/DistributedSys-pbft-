package com.example.pbft.utils;

import java.security.*;
import java.util.Base64;

public class CryptoUtils {

    private static final String ALGORITHM = "SHA256withRSA";
    public static KeyPair generateKeyPair() throws NoSuchAlgorithmException { 
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }
    
    public static KeyPair generateKeyPair(String seed) throws NoSuchAlgorithmException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed(seed.getBytes());
        generator.initialize(2048, random);
        return generator.generateKeyPair();
    }

    public static byte[] sign(PrivateKey privateKey, byte[] data) throws Exception {
        Signature privateSignature = Signature.getInstance(ALGORITHM);
        privateSignature.initSign(privateKey);
        privateSignature.update(data);
        return privateSignature.sign();
    }

    public static boolean verify(PublicKey publicKey, byte[] data, byte[] signature) throws Exception {
        Signature publicSignature = Signature.getInstance(ALGORITHM);
        publicSignature.initVerify(publicKey);
        publicSignature.update(data);
        return publicSignature.verify(signature);
    }
    
    public static String encodeKey(Key key) {
        return Base64.getEncoder().encodeToString(key.getEncoded());
    }
}