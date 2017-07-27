#!/bin/bash

## Generates a certificate chain
## Copied from https://gist.github.com/yamen/af6ec8cc86e4f40a87ea

KEYSTORE_DIR=`dirname $0`/../keystore
ROOT_KS=$KEYSTORE_DIR/root.jks
CA_KS=$KEYSTORE_DIR/ca.jks
SERVER_KS=$KEYSTORE_DIR/server-keystore.jks
SERVER_TS=$KEYSTORE_DIR/server-truststore.jks

mkdir -p $KEYSTORE_DIR

echo "===================================================="
echo "Creating fake third-party chain root -> ca"
echo "===================================================="

# generate private keys (for root and ca)

keytool -genkeypair -alias root -dname cn=root -validity 10000 -keyalg RSA -keysize 2048 -ext bc:c -keystore $ROOT_KS -keypass password -storepass password
keytool -genkeypair -alias ca -dname cn=ca -validity 10000 -keyalg RSA -keysize 2048 -ext bc:c -keystore $CA_KS -keypass password -storepass password

# generate root certificate

keytool -exportcert -rfc -keystore $ROOT_KS -alias root -storepass password > root.pem

# generate a certificate for ca signed by root (root -> ca)

keytool -keystore $CA_KS -storepass password -certreq -alias ca \
| keytool -keystore $ROOT_KS -storepass password -gencert -alias root -ext bc=0 -ext san=dns:ca -rfc > ca.pem

# import ca cert chain into $CA_KS

keytool -keystore $CA_KS -storepass password -importcert -trustcacerts -noprompt -alias root -file root.pem
keytool -keystore $CA_KS -storepass password -importcert -alias ca -file ca.pem

echo  "===================================================================="
echo  "Fake third-party chain generated. Now generating $SERVER_KS ..."
echo  "===================================================================="

# generate private keys (for server)

keytool -genkeypair -alias server -dname cn=server -validity 10000 -keyalg RSA -keysize 2048 -keystore $SERVER_KS -keypass password -storepass password

# generate a certificate for server signed by ca (root -> ca -> server)

keytool -keystore $SERVER_KS -storepass password -certreq -alias server \
| keytool -keystore $CA_KS -storepass password -gencert -alias ca -ext ku:c=dig,keyEnc -ext san=dns:localhost -ext eku=sa,ca -rfc > server.pem

# import server cert chain into $SERVER_KS

keytool -keystore $SERVER_KS -storepass password -importcert -trustcacerts -noprompt -alias root -file root.pem
keytool -keystore $SERVER_KS -storepass password -importcert -alias ca -file ca.pem
keytool -keystore $SERVER_KS -storepass password -importcert -alias server -file server.pem

echo "================================================="
echo "Keystore generated. Now generating truststore $SERVER_TS ..."
echo "================================================="

# import server cert chain into $SERVER_TS

keytool -keystore $SERVER_TS -storepass password -importcert -trustcacerts -noprompt -alias root -file root.pem
keytool -keystore $SERVER_TS -storepass password -importcert -alias ca -file ca.pem
keytool -keystore $SERVER_TS -storepass password -importcert -alias server -file server.pem

echo "Cleaning up"
rm -f $KEYSTORE_DIR/*.pem
