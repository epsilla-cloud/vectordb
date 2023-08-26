#!/bin/bash


## Configurations
STARTUP_FILE=".startup_file"
CONFIG_URL="https://config.epsilla.com/candidate.json"
QUERY_URL="https://ifconfig.co/ip"                           #"https://api.ipify.org"

SENTRY_DSN=`curl $CONFIG_URL | grep sentry | awk -F '"' '{print $(NF-1)}'`
SENTRY_HOST=`echo $SENTRY_DSN | sed -e "s/[^/]*\/\/\([^@]*@\)\?\([^:/]*\).*/\2/"`
SENTRY_SECRET=`echo $SENTRY_DSN | cut -d '/' -f3 | cut -d '@' -f1`
PROTOCOL=`echo $SENTRY_DSN | cut -d ':' -f1`
PROJECT_ID=`echo $SENTRY_DSN | cut -d '/' -f4`

TIMESTAMP=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
HOSTNAME=`hostname --long`
INTERNAL_IP=`hostname -i`
EXTERNAL_IP=`curl $QUERY_URL` 


## Start Up
if [ ! -f "${STARTUP_FILE}" ]; then
  curl -X POST \
  -H 'Content-Type: application/json' \
  -H "X-Sentry-Auth: Sentry sentry_version=7, sentry_key=${SENTRY_SECRET}, sentry_client=epsilla-docker/1.0" \
  "${PROTOCOL}://${SENTRY_HOST}/api/${PROJECT_ID}/store/" \
  --data "{
    \"platform\": \"docker\",
    \"level\": \"info\",
    \"logger\": \"docker\",
    \"server_name\": \"${HOSTNAME}\",
    \"tags\": {
      \"version\": \"latest\",
      \"internal_ip\": \"${INTERNAL_IP}\",
      \"external_ip\": \"${EXTERNAL_IP}\",
      \"timestamp\": \"${TIMESTAMP}\"
    },
    \"message\": {
      \"message\": \"Epsilla VectorDB starts up at ${EXTERNAL_IP}\"
    }
  }";
  touch ${STARTUP_FILE};
  echo "${TIMESTAMP}" > ${STARTUP_FILE};
fi


## HeartBeat
DATE=`date -u +"%Y-%m-%dT%H:%M"`
DATE_TAG=${DATE%?}0
curl -X POST \
-H 'Content-Type: application/json' \
-H "X-Sentry-Auth: Sentry sentry_version=7, sentry_key=${SENTRY_SECRET}, sentry_client=epsilla-docker/1.0" \
"${PROTOCOL}://${SENTRY_HOST}/api/${PROJECT_ID}/store/" \
--data "{
  \"level\": \"info\",
  \"server_name\": \"${HOSTNAME}\",
  \"tags\": {
    \"internal_ip\": \"${INTERNAL_IP}\",
    \"external_ip\": \"${EXTERNAL_IP}\",
    \"heart_beat\": \"${DATE_TAG}\"
  },
  \"message\": {
    \"message\": \"HeartBeat\"
  }
}"

