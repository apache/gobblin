
.PHONY: build
build:
	rm -rf apache-gobblin-incubating-bin-0.15.0.tar.gz
	sudo rm -rf /a/gobblin*
	# ./gradlew -xrat -xtest -xcheck -xjavadoc -xjavadocJar -xcheckstyleMain -xcheckstyleTest build
	# ./gradlew -xrat -xtest -xcheck -xjavadoc -xjavadocJar -xcheckstyleMain -xcheckstyleTest :gobblin-distribution:clean
	./gradlew -xrat -xtest -xcheck -xjavadoc -xjavadocJar -xcheckstyleMain -xcheckstyleTest :gobblin-distribution:clean :gobblin-distribution:buildDistributionTar
	tar xf apache-gobblin-incubating-bin-0.15.0.tar.gz -C /a/
	\cp /opt/playground/gobblin-dist/bin/gobblin /a/gobblin-dist/bin/gobblin

.PHONY: run
run:
	sudo rm -rf /a/gobblin-kuber
	/a/gobblin-dist/bin/gobblin cli run wikipedia -mrMode gobblin

