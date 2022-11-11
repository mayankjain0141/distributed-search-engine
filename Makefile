build:
	rm -rf output
	mvn clean -f pom.xml
	mvn install -f pom.xml
run:
	spark-submit --class Driver target/Cloud_Assignment_1-1.0.jar
