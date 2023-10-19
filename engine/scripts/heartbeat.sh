#!/bin/bash


## Configurations
STARTUP_FILE=".startup_file"
CONFIG_URL="https://config.epsilla.com/candidate.json"
QUERY_URL="https://api.ipify.org"

RELEASE_VERSION=${ENV_RELEASE_VERSION:-latest} 


SENTRY_DSN=`curl -s $CONFIG_URL | grep heartbeat | awk -F '"' '{print $(NF-1)}'`
SENTRY_HOST=`echo $SENTRY_DSN | sed -e "s/[^/]*\/\/\([^@]*@\)\?\([^:/]*\).*/\2/"`
SENTRY_SECRET=`echo $SENTRY_DSN | cut -d '/' -f3 | cut -d '@' -f1`
PROTOCOL=`echo $SENTRY_DSN | cut -d ':' -f1`
PROJECT_ID=`echo $SENTRY_DSN | cut -d '/' -f4`

POSTHOG_HOST=`curl -s $CONFIG_URL | grep posthog | cut -d '@' -f1 | cut -d '"' -f4`
POSTHOG_API_KEY=`curl -s $CONFIG_URL | grep posthog | cut -d '@' -f2 | cut -d '"' -f1`

TIMESTAMP=`date -u +"%Y-%m-%dT%H:%M:%SZ"`
HOSTNAME=`hostname --long`
INTERNAL_IP=`hostname -i`
if [ ! -f "${STARTUP_FILE}" ]; then
  EXTERNAL_IP=`curl -s ${QUERY_URL} -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36' --compressed` 
else
  EXTERNAL_IP=`cat ${STARTUP_FILE}`
fi

DISTINCT_ID=`md5sum <<< "${HOSTNAME}-${INTERNAL_IP}-${EXTERNAL_IP}" | cut -d ' ' -f1`

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
      \"version\": \"${RELEASE_VERSION}\",
      \"internal_ip\": \"${INTERNAL_IP}\",
      \"external_ip\": \"${EXTERNAL_IP}\",
      \"timestamp\": \"${TIMESTAMP}\",
      \"distinct_id\": \"${DISTINCT_ID}\"
    },
    \"message\": {
      \"message\": \"Epsilla VectorDB starts up at ${EXTERNAL_IP}\"
    }
  }";
  curl -X POST -L --header "Content-Type: application/json" \
  "${POSTHOG_HOST}/capture/" \
  --data "{
    \"event\": \"VectorDB started\",
    \"api_key\": \"${POSTHOG_API_KEY}\",
    \"distinct_id\": \"${DISTINCT_ID}\",
    \"properties\": {
      \"version\": \"latest\",
      \"internal_ip\": \"${INTERNAL_IP}\",
      \"external_ip\": \"${EXTERNAL_IP}\"
    }
  }";
  touch ${STARTUP_FILE};
  echo "${EXTERNAL_IP}" > ${STARTUP_FILE};
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
    \"heart_beat\": \"${DATE_TAG}\",
    \"distinct_id\": \"${DISTINCT_ID}\"
  },
  \"user\": {
    \"username\": \"${HOSTNAME}-${INTERNAL_IP}-${EXTERNAL_IP}\",
    \"ip_address\": \"${EXTERNAL_IP}\"
  },
  \"message\": {
    \"message\": \"HeartBeat\"
  }
}"
curl -X POST -L --header "Content-Type: application/json" \
  "${POSTHOG_HOST}/capture/" \
  --data "{
    \"event\": \"VectorDB heartbeat\",
    \"api_key\": \"${POSTHOG_API_KEY}\",
    \"distinct_id\": \"${DISTINCT_ID}\",
    \"properties\": {
      \"version\": \"latest\",
      \"internal_ip\": \"${INTERNAL_IP}\",
      \"external_ip\": \"${EXTERNAL_IP}\"
    }
  }";

