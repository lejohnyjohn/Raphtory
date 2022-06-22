lotr-assembly:
	sbt examplesLotr/assembly

lotr-docker:
	 docker build -f bin/docker/lotr/Dockerfile -t raphtory-lotr .

run-lotr: lotr-assembly lotr-docker
	docker-compose -f bin/docker/lotr/docker-compose.yml up