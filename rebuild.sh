#/bin/bash
mvn compile
mvn package -Dmaven.test.skip=true
