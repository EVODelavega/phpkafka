Master build: [![Build Status](https://travis-ci.org/EVODelavega/phpkafka.svg?branch=master)](https://travis-ci.org/EVODelavega/phpkafka)

Dev build: [![Build Status](https://travis-ci.org/EVODelavega/phpkafka.svg?branch=consume-with-meta)](https://travis-ci.org/EVODelavega/phpkafka)

SRP build: [![Build Status](https://travis-ci.org/EVODelavega/phpkafka.svg?branch=feature%2FSRP)](https://travis-ci.org/EVODelavega/phpkafka)

## Common issues:

Here's a short list of common issues people run into when installing this extension (only 1 issue so far)

#### _"Unable to load dynamic library '/usr/lib64/php/modules/kafka.so' - librdkafka.so.1"_

What this, basically means is that PHP can't find the shared object (librdkafka) anywhere. Thankfully, the fix is trivial:
First, make sure you've actually compiled and installed librdkafka. Then run these commands:

```bash
sudo updatedb
locate librdkafka.so.1 # locate might not exist on some systems, like slackware, which uses slocate
```

The output should show a full path to the `librdkafka.so.1` file, probably _"/usr/local/lib/librdkafka.so.1"_. Edit `/etc/ld.so.conf` to make sure _"/usr/local/lib"_ is included when searching for libraries. Either add id directly to the aforementioned file, or if your system uses a /etc/ld.so.conf.d/ directory, create a new .conf file there:

```bash
sudo touch /etc/ld.so.conf.d/librd.conf
echo "/usr/local/lib" >> /etc/ld.so.conf.d/librd.conf
```

Or simply type `vim /etc/ld.so.conf.d/librd.conf`, when the editor opens, tap _":"_ (colon), and run the command `read !locate librdkafka.so.1`, delete the filename from the path (move your cursor to the last `/` of the line that just appeared in the file and type `d$` (delete until end of line). Save and close the file (`:wq`).

Once updated, run the following:

```bash
sudo ldconfig
```


_Note:_

Whatever gets merged into the master branch should work just fine. The main dev build is where small tweaks, bugfixes and minor improvements are tested (ie sort-of beta branch).

The SRP build is a long-term dev branch, where I'm currently in the process of separating the monolithic `Kafka` class into various logical sub-classes (a `KafkaTopic` class, perhaps a `KafkaMeta` object, `KafkaConfig` is another candidate...) to make this extension as intuitive as I can.

#This fork is still being actively developed

Given that the original repo is no longer supported by the author, I've decided to keep working at this PHP-Kafka extension instead.
The branch where most of the work is being done is the `consume-with-meta` branch.

Changes that have happened thusfar:

* Timeout when disconnecting is reduced (significantly)
* Connections can be closed as you go
* The librdkafka meta API is used
* New methods added (`Kafka::getTopics`, `Kafka::getPartitionsFor($topic)` most notable additions)
* `Kafka::set_partition` is deprecated, in favour of the more PSR-compliant `Kafka::setPartition` method
* A PHP stub was added for IDE code-completion
* Argument checks were added, and exceptions are thrown in some places
* Class constants for an easier API (`Kafka::OFFSET_*`)
* The extension logged everything in `/var/etc/syslog`, this is still the default behaviour (as this extension is under development), but can be turned off (`Kafka::setLogLevel(Kafka::LOG_OFF)`)
* Exceptions (`KafkaException`) in case of errors (still work in progress, though)
* Thread-safe Kafka connections
* Easy configuration: passing an array of options to the constructor, `setBrokers` or `setOptions` method like you would with `PDO`
* Compression support added (when produing messages, a compressed message is returned _as-is_)
* Each instance holds 2 distinct connections (at most): a producer and a consumer
* CI (travis), though there is a lot of work to be done putting together useful tests

Changes that are on the _TODO_ list include:

* Separating kafka meta information out into a separate class (`KafkaTopic`  and `KafkaMessage` classes)
* Allow PHP to determine what the timeouts should be (mainly when disconnecting, or producing messages) (do we still need this?)
* Add custom exceptions (partially done)
* Overall API improvements (!!)
* Performance - it's what you make of it (test results varied from 2 messages/sec to 2.5 million messages per second - see examples below)
* Adding tests to the build (very much a work in progress)
* PHP7 support

All help is welcome, of course...


PHP extension for **Apache Kafka 0.8**. It's built on top of kafka C driver ([librdkafka](https://github.com/edenhill/librdkafka/)).
This extension requires the version 0.8.6 (ubuntu's librdkafka packages won't do it, they do not implement the meta API yet).

IMPORTANT: Library is in heavy development and some features are not implemented yet.

Requirements:
-------------
Download and install [librdkafka](https://github.com/edenhill/librdkafka/). Run `sudo ldconfig` to update shared libraries.

Installing PHP extension:
----------
```bash
phpize
./configure --enable-kafka
make
sudo make install
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/conf.d/kafka.ini'
#For CLI mode:
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/cli/conf.d/20-kafka.ini'
```

Examples:
--------
```php
// Produce a message
$kafka = new Kafka("localhost:9092");
$kafka->produce("topic_name", "message content");
//get all the available partitions
$partitions = $kafka->getPartitionsForTopic('topic_name');
//use it to OPTIONALLY specify a partition to consume from
//if not, consuming IS slower. To set the partition:
$kafka->setPartition($partitions[0]);//set to first partition
//then consume, for example, starting with the first offset, consume 20 messages
$msg = $kafka->consume("topic_name", Kafka::OFFSET_BEGIN, 20);
var_dump($msg);//dumps array of messages
```

A more complete example of how to use this extension if performance is what you're after:

```php
$kafka = new Kafka(
    'broker-1:9092,broker-2:9092',
    [
        Kafka::LOGLEVEL         => Kafka::LOG_OFF,//while in dev, default is Kafka::LOG_ON
        Kafka::CONFIRM_DELIVERY => Kafka::CONFIRM_OFF,//default is Kafka::CONFIRM_BASIC
        Kafka::RETRY_COUNT      => 1,//default is 3
        Kafka::RETRY_INTERVAL   => 25,//default is 100
    ]
);
$fh = fopen('big_data_file.csv', 'r');
if (!$fh)
    exit(1);
$count = 0;
$lines = [];
while ($line = fgets($fh, 2048))
{
    $lines[] = trim($line);
    ++$count;
    if ($count >= 200)
    {
        $kafka->produceBatch('my_topic', $lines);
        $lines = [];
        $count = 0;
        //in theory, the next bit is optional, but Kafka::disconnect
        //waits for the out queue to be empty before closing connections
        //it's a way to sort-of ensure messages are delivered, even though Kafka::CONFIRM_DELIVERY
        //was set to Kafka::CONFIRM_OFF... This approach can be used to speed up your code
        $kafka->disconnect(Kafka::MODE_PRODUCER);//disconnect the producer
    }
}
if ($count)
{
    $kafka->produceBatch('my_topic', $lines);
}
$kafka->disconnect();//disconnects all opened connections, in this case, only a producer connection will exist, though
```

I've used code very similar to the code above to produce ~3 million messages, and got an average throughput rate of 2000 messages/second.
Removing the disconnect call, or increasing the batches to produce will change the rate at which messages get produced.

Not disconnecting at all yielded the best performance (by far): 2.5 million messages in just over 1 second (though depending on the output buffer, and how kafka is set up to handle full produce-queue's, this is not to be recommended!).
