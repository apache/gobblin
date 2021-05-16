IMAGE_NAME := trv-local-java-runner
IMAGE_REMOTE := default-registry.localhost:5000/${IMAGE_NAME}:v0.2

.PHONY: clean
clean:
	docker rmi ${IMAGE_NAME} 2>/dev/null || true
	docker rmi ${IMAGE_REMOTE} 2>/dev/null || true
	rm -rf runner.jar

.PHONY: get-jar
get-jar:
	cp ../runner/build/libs/runner.jar .

.PHONY: build
build: clean get-jar
	docker build -t ${IMAGE_REMOTE} .

#.PHONY: tag
#tag:
#	docker tag ${IMAGE_NAME}:latest ${IMAGE_REMOTE}

.PHONY: push
push:
	 docker push ${IMAGE_REMOTE}

.PHONY: k3d-reg
k3d-reg:
	k3d registry create default-registry.localhost -p 5000

.PHONY: k3d-cluster
k3d-cluster:
	k3d cluster create --volume /a:/a --registry-use=k3d-default-registry.localhost:5000


.PHONY: run
run:
ifeq ($(JARS),)
	@echo 'JARS variable not set'
	@exit 1
endif
	docker run --rm -it -v ${JARS}:/jars -p 8080:8080 ${IMAGE_REMOTE} /jars


.PHONY: dummy
dummy:
	docker run --rm -it \
		-v $(PWD)/../dummy/build/libs:/jars \
		${IMAGE_REMOTE} \
		/jars \
		com.trivago.de.kubernetes.javarunner.dummy.Dummy \
		foo bar baz


.PHONY: dummy-failing
dummy-failing:
	docker run --rm -it \
		-v $(PWD)/../dummy/build/libs:/jars \
		${IMAGE_REMOTE} \
		/jars \
		com.trivago.de.kubernetes.javarunner.dummy.DummyFailing \
		foo bar baz


