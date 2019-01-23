#!/bin/bash

# Creates a good (signed by a CA) and a bad certificate. Can be used by SSL_KEY_EXCHANGE to test whether rogue
# members can join a cluster, send messages to it etc.
# Author: Tristan Tarrant


KEYSTORE_DIR=`dirname $0`/../keystore
CA_KS=$KEYSTORE_DIR/ca.jks
ROGUE_CA_KS=$KEYSTORE_DIR/rogue-ca.jks
GOOD_SERVER_KS=$KEYSTORE_DIR/good-server.jks
ROGUE_SERVER_KS=$KEYSTORE_DIR/rogue-server.jks
CA_CERT=$KEYSTORE_DIR/ca.cer
ROGUE_CA_CERT=$KEYSTORE_DIR/rogue-ca.cer
SERVER_CSR=$KEYSTORE_DIR/server.csr
SERVER_CER=$KEYSTORE_DIR/server.cer
ROGUE_CSR=$KEYSTORE_DIR/rogue.csr
ROGUE_CER=$KEYSTORE_DIR/rogue.cer
REAL_DIR=`realpath $KEYSTORE_DIR`


mkdir -p $KEYSTORE_DIR

# generate CA
keytool -genkeypair -alias ca -dname cn=ca -validity 10000 -keyalg RSA -keysize 2048 -ext bc:c -keystore $CA_KS -keypass password -storepass password

# generate rogue CA
keytool -genkeypair -alias rogue_ca -dname cn=rogue_ca -validity 10000 -keyalg RSA -keysize 2048 -ext bc:c -keystore $ROGUE_CA_KS -keypass password -storepass password


# export CA certificate
keytool -exportcert -keystore $CA_KS -alias ca -storepass password -keypass password -file $CA_CERT

# export rogue CA certificate
keytool -exportcert -keystore $ROGUE_CA_KS -alias rogue_ca -storepass password -keypass password -file $ROGUE_CA_CERT

# generate a trusted key for the server and sign it with the CA
keytool -genkeypair -alias server -dname cn=server -validity 10000 -keyalg RSA -keysize 2048 -keystore $GOOD_SERVER_KS -keypass password -storepass password
keytool -certreq -alias server -dname cn=server -keystore $GOOD_SERVER_KS -file $SERVER_CSR -keypass password -storepass password
keytool -gencert -alias ca -keystore $CA_KS -infile $SERVER_CSR -outfile $SERVER_CER -keypass password -storepass password

# import the server cert chain into $GOOD_SERVER_KS (CA and server cert)
keytool -importcert -alias ca -keystore $GOOD_SERVER_KS -file $CA_CERT -keypass password -storepass password -noprompt
keytool -importcert -alias server -keystore $GOOD_SERVER_KS -file $SERVER_CER -keypass password -storepass password

# generate an untrusted key for another server (don't sign it)
# keytool -genkeypair -alias rogue -dname cn=rogue -validity 10000 -keyalg RSA -keysize 2048 -keystore $ROGUE_SERVER_KS -keypass password -storepass password


# generate a untrusted key for the rogue and sign it with the rogue CA
keytool -genkeypair -alias rogue -dname cn=rogue -validity 10000 -keyalg RSA -keysize 2048 -keystore $ROGUE_SERVER_KS -keypass password -storepass password
keytool -certreq -alias rogue -dname cn=rogue -keystore $ROGUE_SERVER_KS -file $ROGUE_CSR -keypass password -storepass password
keytool -gencert -alias rogue_ca -keystore $ROGUE_CA_KS -infile $ROGUE_CSR -outfile $ROGUE_CER -keypass password -storepass password

# import the rogue cert chain into $ROGUE_SERVER_KS (CA and rogue cert)
keytool -importcert -alias rogue_ca -keystore $ROGUE_SERVER_KS -file $ROGUE_CA_CERT -keypass password -storepass password -noprompt
keytool -importcert -alias rogue -keystore $ROGUE_SERVER_KS -file $ROGUE_CER -keypass password -storepass password


echo "Generated $GOOD_SERVER_KS and $ROGUE_SERVER_KS in $REAL_DIR"
echo "Cleaning up"
rm -f $KEYSTORE_DIR/*.csr $KEYSTORE_DIR/*.cer $KEYSTORE_DIR/*ca.jks
