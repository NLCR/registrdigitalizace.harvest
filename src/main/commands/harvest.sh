#!/bin/sh

HARVEST_HOME=
JAVA_OPTS=""

if [ "$HARVEST_HOME" = "" ] ; then
    echo "Missing HARVEST_HOME property."
    exit 1
fi

# logging
JAVA_OPTS="$JAVA_OPTS -Djava.util.logging.config.file=$HARVEST_HOME/conf/logging.properties"

# config
JAVA_OPTS="$JAVA_OPTS -Dcz.registrdigitalizace.harvest.Harvest.config=$HARVEST_HOME/conf/harvest.properties"

java $JAVA_OPTS -cp "$HARVEST_HOME/lib/*" cz.registrdigitalizace.harvest.Harvest
