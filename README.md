phpkafka
========

PHP extension for **Apache Kafka 0.8**. It's built on top of kafka C driver ([librdkafka](https://github.com/edenhill/librdkafka/)).
It makes persistent connection to kafka broker with non-blocking calls, so it should be very fast.

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
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/fpm/conf.d/kafka.ini'
#For CLI mode:
sudo sh -c 'echo "extension=kafka.so" >> /etc/php5/cli/conf.d/20-kafka.ini'
#For fpm:
sudo service php5-fpm restart
```

Examples:
--------
```php
// Produce a message
$kafka = new Kafka("localhost:9092");
$kafka->produce("topic_name", "message content");
$partition = 1;//specify the partition somehow
//the partition needs to be set, or the consume method will cause a fatal error (C code: exit(1);)
$kafka->set_partition($partition);
$kafka->consume("topic_name", 1172556);
```
