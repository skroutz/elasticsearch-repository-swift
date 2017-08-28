FROM docker.elastic.co/elasticsearch/elasticsearch:5.5.2
ADD ./build/distributions/repository-swift-3.0.0-es5.5.2.zip .
RUN /usr/share/elasticsearch/bin/elasticsearch-plugin install --batch file:///usr/share/elasticsearch/repository-swift-3.0.0-es5.5.2.zip
