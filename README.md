# Zoomap [![Build Status](https://api.travis-ci.org/mcmoe/zoomap.png)](https://travis-ci.org/mcmoe/zoomap) [![Code Coverage](https://codecov.io/github/mcmoe/zoomap/coverage.svg)](https://codecov.io/github/mcmoe/zoomap/coverage.svg)


A Zookeeper backed stateless Java Map implementation using the curator framework

## Why

Would like to use a java map interface as a distributed key value store for configuration.
Apache Zookeeper looked like a good candidate for config.
The Zookeeper client is best used via Apache curator.
Couldn't find any implementations out there that use curator.
So welcome Zoomap.

To improve...
