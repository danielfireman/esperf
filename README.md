# esperf

Is a single-binary multiplatform command line tool to load test and collect metrics from your [ElasticSearch](https://github.com/elastic/elasticsearch) deployment. No need install dependencies or anything. Just download and use. Built with love by danielfireman and friends in Go. Esperf provides:

* A load specification format that allows replaying the shape of the load, not only the requests made
* Ability to perform queries based on a dictionary of words
* Ability to properly handle 503 or 429 http responde codes, respecting the delay suggested by the Retry-After response header field.
* Send query-based load following a constant or Poisson distribution
* Send query-based load replaying slowlog
* Collect the following metrics:
     * Overall CPU load
     * GC time and count (broken by full and young collections)
     * Latency 50, 90, 99, 99.9 percentiles
     * Memory pools usage (broken  by young, survivor and tenured)
     * Throughput and error counters
     
**Disclaimer: a lot in flux.** 

# Install

## Pre-compiled binaries

Pre-compiled binaries for many platforms can be found [here](https://github.com/danielfireman/esperf/releases).

## Source

You need go installed and `GOBIN` in your `PATH`. Once that is done, run the command:

```bash
$ go get -u github.com/danielfireman/esperf
```

## Usage

### Creating synthetic load specification

Bellow we show two ways to generate synthetic load test specifications:

```bash
$ echo '{"query": {"term": {"text": {"value": "Brazil"}}}}' |  ./esperf loadspec gen --arrival_spec=const:5 --duration=5s "http://localhost:9200/wikipediax/_search?search_type=query_then_fetch"
```

The load test described by the spec will last for 5 seconds and the load will follow a constant distribution of 5
requests per second. There is also only one query, which is repeated throughout the load test
execution: `{"query": {"term": {"text": {"value": "Brazil"}}}}`

```bash
$ echo '{"query": {"term": {"text": {"value": "$RDICT"}}}}' |  ./esperf loadspec gen --arrival_spec=poisson:5 --dictionary_file=small_dict.txt --duration=5s "http://localhost:9200/wikipediax/_search?search_type=query_then_fetch"
```

In this case:
* The load test duration is 10s;
* Trigger term queries using randomly selected strings from small_dict.txt dictionary file;
* Request arrival times will be sent according to the Poisson distribution (lambda parameter equals to 5).

### Creating load specification based on slowlogs

Generate a load specification (`slowlogs.loadspec.json`) based on the passed-in slowlogs. Host, index and other query parameters are going to be extracted from slowlogs. The load test specification will preserve the arrival times or queries, trying to mimick the arrival distribution as much as possible.

```bash
cat my_slowlogs.log |  ./esperf loadspec parseslowlog > slowlogs.loadspec.json
```

If you would like to change URL parameters of the query (for instance, replay the loadtests on another host:port).

```bash
cat my_slowlogs.log |  ./esperf loadspec parseslowlog "http://localhost:9200/wikipediax/_search?search_type=query_then_fetch" > slowlogs.loadspec.json
```

### Executing a load test specifications (A.K.A. firing the load)

The following command runs a load test based on the passed in specification. All the results will be placed at the
current directory (`$PWD`) and statistics will be collected each second from http://localhost:9200.

```bash
cat poisson.loadspec.json | ./esperf replay --mon_host=http://localhost:9200 --mon_interval=1s --results_path=$PWD
```

### Hit count

Sometimes one would be interested on finding the number of hits of some terms. For instance, that could be useful to
identify potentially heavy queries.

To generate a list of terms ordered descending by number of hits:

```bash
echo '{"size":0, "query": {"match": {"text": "$RDICT"}}}' |  ./esperf counthits --dictionary_file=small_dict.txt "http://localhost:9200/wikipediax/_search?search_type=query_then_fetch"
```

\* according to elasticsearch documentation, the number of hits is the total number of documents matching our search criteria


# Why esperf exists?

When researching for tools to load test ES I was quickly reminded by ES REST beauties and pointed out to [JMeter ](http://jmeter.apache.org/) or tools alike. As I needed to conduct experiments with very specific needs, I found that would be easier to build a tool myself than work around.

Also, I couldn't find a spec format that would respect the distribution of the arrival times. In particular, when parsing slowlogs, I would like to replay that respecting as much as possible the arrival times that were collected. I took a look at [Vegeta](https://github.com/tsenart/vegeta/) and [Yandex-Tank](https://github.com/yandex/yandex-tank) formats. 

On the metrics side, I took a look at some ES plugins (i.e. [Marvel](https://www.elastic.co/downloads/marvel)) but I also bumped into some restrictions, for instance tune the metrics collection interval, pic and choose metrics and have access to raw data (CSV) to play with them in platforms like [R](https://www.r-project.org/) :heart:

Non-exaustive list of sources of inspiration:

* [kosho/esperf](https://github.com/kosho/esperf): loadspec gen inspiration.
* [coxx/es-slowlog](https://github.com/coxx/es-slowlog): ideas and pointers to other loadspec file formats
* [tsenart/vegeta](https://github.com/tsenart/vegeta/) and [yandex/yandex-tank](https://github.com/yandex/yandex-tank): simple loadtest spec formats
* [wg/wrk](https://github.com/wg/wrk): powerful [LUA](https://www.lua.org/) scripting for intercepting requests and responses

# Thanks

* [Péter Szilágyi](https://github.com/karalabe) for [Xgo](https://github.com/karalabe/xgo), which turned cross-compiling go programs into a breeze
* [Steve Francia](https://github.com/spf13) and others for [Cobra](https://github.com/spf13/cobra), great framework for building command-line tools in go

