//package org.apache.solr.util;
//
//import java.security.Provider;
//
//public class SolrTestNonSecureRandomProvider extends Provider {
//
//        public SolrTestNonSecureRandomProvider() {
//            super("LinuxPRNG",
//                    1.0,
//                    "A Test only, non secure provider");
//            put("SecureRandom.SHA1PRNG", NotSecurePseudoRandomSpi.class.getName());
//            put("SecureRandom.NativePRNG", NotSecurePseudoRandomSpi.class.getName());
//            put("SecureRandom.DRBG", NotSecurePseudoRandomSpi.class.getName());
//
//
//            put("SecureRandom.SHA1PRNG ThreadSafe", "true");
//            put("SecureRandom.NativePRNG ThreadSafe", "true");
//            put("SecureRandom.DRBG ThreadSafe", "true");
//
//
//            put("SecureRandom.SHA1PRNG ImplementedIn", "Software");
//            put("SecureRandom.NativePRNG ImplementedIn", "Software");
//            put("SecureRandom.DRBG ImplementedIn", "Software");
//        }
//    }