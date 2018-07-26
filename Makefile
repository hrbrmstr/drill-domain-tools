.PHONY: udf deps install restart

DRILL_HOME ?= /usr/local/drill

udf:
	mvn --quiet clean package -DskipTests

install:
	cp target/drill-domain-tools*.jar ${DRILL_HOME}/jars/3rdparty && \
	cp deps/crawler-commons-0.10.jar ${DRILL_HOME}/jars/3rdparty

restart:
	drillbit.sh restart

deps:
	mvn --quiet dependency:copy-dependencies -DoutputDirectory=deps
