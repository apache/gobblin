
IMAGE_NAME := trv-net-java-runner

.PHONY: clean
clean:
	docker rmi ${IMAGE_NAME} || true
	rm -rf app.jar

.PHONY: get-jar
get-jar:
	cp ../app/build/libs/app.jar .

.PHONY: build
build: clean get-jar
	docker build -t ${IMAGE_NAME} .


.PHONY: run
run:
ifeq ($(JARS),)
	@echo 'JARS variable not set'
	@exit 1
endif
	docker run --rm -it -v ${JARS}:/jars -p 8080:8080 ${IMAGE_NAME}


.PHONY: dummy
dummy:
	docker run --rm -it -v $(PWD)/../dummy/build/libs:/jars -p 8080:8080 ${IMAGE_NAME}

