build:
	mvn clean -f pom.xml
	mvn install -f pom.xml

clean:
	rm -rf output_*

run:
	make build
	spark-submit --class Driver target/Cloud_Assignment_1-1.0.jar

search-phrase:
	rm -rf output_search_phrase tempresult
	make run

search-word:
	rm -rf output_search_word
	make run

inverted-index:
	rm -rf output_inverted_index
	make run
