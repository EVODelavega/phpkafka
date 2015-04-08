/**
 *  Copyright 2013-2014 Patrick Reilly.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <php.h>
#include "kafka.h"
#include <php_kafka.h>
#include <inttypes.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <time.h>
#include "kafka.h"
#include "librdkafka/rdkafka.h"

static int run = 1;
static int log_level = 1;
static rd_kafka_t *rk;
static rd_kafka_type_t rk_type;
static int exit_eof = 1; //Exit consumer when last message
char *brokers = "localhost:9092";
int64_t start_offset = 0;
int partition = RD_KAFKA_PARTITION_UA;

void kafka_connect(char *brokers)
{
    kafka_setup(brokers);
}

void kafka_set_log_level( int ll )
{
    log_level = ll;
}

//return 1 if rd is not NULL
int kafka_is_connected( void )
{
    if (rk == NULL)
        return 0;
    return 1;
}

void kafka_msg_delivered (rd_kafka_t *rk,
                           void *payload, size_t len,
                           int error_code,
                           void *opaque, void *msg_opaque)
{
    if (error_code && log_level) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - Message delivery failed: %s",
                rd_kafka_err2str(error_code));
    }
}

void kafka_stop(int sig)
{
    run = 0;
    fclose(stdin); /* abort fgets() */
    rd_kafka_destroy(rk);
    rk = NULL;
}

void kafka_err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    if (log_level) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - ERROR CALLBACK: %s: %s: %s\n",
            rd_kafka_name(rk), rd_kafka_err2str(err), reason);
    }
    kafka_stop(err);
}

rd_kafka_t *kafka_set_connection(rd_kafka_type_t type, const char *b)
{
    rd_kafka_t *r = NULL;
    char *tmp = brokers;
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (!(r = rd_kafka_new(type, conf, errstr, sizeof(errstr)))) {
        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
        }
        exit(1);
    }
    /* Add brokers */
    if (rd_kafka_brokers_add(r, b) == 0) {
        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "php kafka - No valid brokers specified");
        }
        exit(1);
    }
    /* Set up a message delivery report callback.
     * It will be called once for each message, either on successful
     * delivery to broker, or upon failure to deliver to broker. */
    rd_kafka_conf_set_dr_cb(conf, kafka_msg_delivered);
    rd_kafka_conf_set_error_cb(conf, kafka_err_cb);

    if (log_level) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - using: %s", brokers);
    }
    return r;
}

void kafka_set_partition(int partition_selected)
{
    partition = partition_selected;
}

void kafka_setup(char* brokers_list)
{
    brokers = brokers_list;
}

void kafka_destroy(rd_kafka_t *r, int timeout)
{
    if (r == NULL || (void *)r == (void *)rk)
    {//NULL passed, or r == global pointer variable
        r = rk;
        rk = NULL;
    }
    if(r != NULL) {
        rd_kafka_destroy(r);
        //this wait is blocking PHP
        //not calling it will yield segfault, though
        rd_kafka_wait_destroyed(timeout);
        r = NULL;
    }
}

static void kafka_init( rd_kafka_type_t type )
{
    if (rk_type != type) {
        kafka_destroy(NULL, 1);
    }
    if (rk == NULL)
    {
        char errstr[512];
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        if (!(rk = rd_kafka_new(type, conf, errstr, sizeof(errstr)))) {
            if (log_level) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
            }
            exit(1);
        }
        /* Add brokers */
        if (rd_kafka_brokers_add(rk, brokers) == 0) {
            if (log_level) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO, "php kafka - No valid brokers specified");
            }
            exit(1);
        }
        /* Set up a message delivery report callback.
         * It will be called once for each message, either on successful
         * delivery to broker, or upon failure to deliver to broker. */
        rd_kafka_conf_set_dr_cb(conf, kafka_msg_delivered);
        rd_kafka_conf_set_error_cb(conf, kafka_err_cb);

        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - using: %s", brokers);
        }
    }
}

void kafka_produce(rd_kafka_t *r, char* topic, char* msg, int msg_len)
{

    signal(SIGINT, kafka_stop);
    signal(SIGPIPE, kafka_stop);

    rd_kafka_topic_t *rkt;
    int partition = RD_KAFKA_PARTITION_UA;

    rd_kafka_topic_conf_t *topic_conf;

    if (r == NULL) {
        kafka_init(RD_KAFKA_PRODUCER);
        r = rk;
    }

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(r, topic, topic_conf);

    if (rd_kafka_produce(rkt, partition,
                     RD_KAFKA_MSG_F_COPY,
                     /* Payload and length */
                     msg, msg_len,
                     /* Optional key and its length */
                     NULL, 0,
                     /* Message opaque, provided in
                      * delivery report callback as
                      * msg_opaque. */
                     NULL) == -1) {
       if (log_level) {
           openlog("phpkafka", 0, LOG_USER);
           syslog(LOG_INFO, "phpkafka - %% Failed to produce to topic %s "
               "partition %i: %s",
               rd_kafka_topic_name(rkt), partition,
               rd_kafka_err2str(
               rd_kafka_errno2err(errno)));
        }
        rd_kafka_poll(r, 1);
    }

    /* Poll to handle delivery reports */
    rd_kafka_poll(r, 1);

    /* Wait for messages to be delivered */
    while (run && rd_kafka_outq_len(r) > 0)
      rd_kafka_poll(r, 10);

    rd_kafka_topic_destroy(rkt);
}

static rd_kafka_message_t *msg_consume(rd_kafka_message_t *rkmessage,
       void *opaque)
{
    if (rkmessage->err) {
        if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            if (log_level) {
                openlog("phpkafka", 0, LOG_USER);
                syslog(LOG_INFO,
                    "phpkafka - %% Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(rkmessage->rkt),
                    rkmessage->partition, rkmessage->offset);
            }
            if (exit_eof)
                run = 0;
            return NULL;
        }
        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - %% Consume error for topic \"%s\" [%"PRId32"] "
                "offset %"PRId64": %s\n",
                rd_kafka_topic_name(rkmessage->rkt),
                rkmessage->partition,
                rkmessage->offset,
                rd_kafka_message_errstr(rkmessage)
            );
            return NULL;
        }
    }

    return rkmessage;
}

//get topics + partition count
void kafka_get_topics(rd_kafka_t *r, zval *return_value)
{
    int i;
    const struct rd_kafka_metadata *meta = NULL;
    if (r == NULL)
    {
        kafka_init(RD_KAFKA_CONSUMER);
        r = rk;
    }
    if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_metadata(r, 1, NULL, &meta, 200)) {
        for (i=0;i<meta->topic_cnt;++i) {
            add_assoc_long(
               return_value,
               meta->topics[i].topic,
               (long) meta->topics[i].partition_cnt
            );
        }
    }
    if (meta) {
        rd_kafka_metadata_destroy(meta);
    }
}

static
int kafka_partition_count(rd_kafka_t *r, const char *topic)
{
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t *conf;
    int i;//C89 compliant
    //connect as consumer if required
    if (r == NULL)
    {
        kafka_init(RD_KAFKA_CONSUMER);
        r = rk;
    }
    /* Topic configuration */
    conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(r, topic, conf);
    //metadata API required rd_kafka_metadata_t** to be passed
    const struct rd_kafka_metadata *meta = NULL;
    if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_metadata(r, 0, rkt, &meta, 200))
        i = (int) meta->topics->partition_cnt;
    else
        i = 0;
    if (meta) {
        rd_kafka_metadata_destroy(meta);
    }
    return i;
}

//get the available partitions for a given topic
void kafka_get_partitions(rd_kafka_t *r, zval *return_value, char *topic)
{
    int i, count = kafka_partition_count(r, topic);
    for (i=0;i<count;++i) {
        add_next_index_long(return_value, i);
    }
}

/**
 * @brief Get all partitions for topic and their beginning offsets, useful
 * if we're consuming messages without knowing the actual partition beforehand
 * @param int **partitions should be pointer to NULL, will be allocated here
 * @param const char * topic topic name
 * @return int (0 == success, all others indicate failure)
 */
int kafka_partition_offsets(rd_kafka_t *r, long **partitions, const char *topic)
{
    rd_kafka_topic_t *rkt;
    rd_kafka_topic_conf_t *conf;
    int i = 0;
    //make life easier, 1 level of indirection...
    long *values = *partitions;
    //connect as consumer if required
    if (r == NULL)
    {
        kafka_init(RD_KAFKA_CONSUMER);
        r = rk;
    }
    /* Topic configuration */
    conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(r, topic, conf);
    const struct rd_kafka_metadata *meta = NULL;
    if (RD_KAFKA_RESP_ERR_NO_ERROR == rd_kafka_metadata(r, 0, rkt, &meta, 5))
    {
        values = realloc(values, meta->topics->partition_cnt * sizeof *values);
        if (values == NULL) {
            *partitions = values;//possible corrupted pointer now
            //free metadata, return error
            rd_kafka_metadata_destroy(meta);
            return -1;
        }
        for (i;i<meta->topics->partition_cnt;++i) {
            //consume_start returns 0 on success
            values[i] = -1;//initialize memory
            if (rd_kafka_consume_start(rkt, i, RD_KAFKA_OFFSET_BEGINNING))
                continue;
            rd_kafka_message_t *rkmessage = rd_kafka_consume(rkt, i, 900),
                    *rkmessage_return;

            if (!rkmessage) /* timeout */
              continue;

            rkmessage_return = msg_consume(rkmessage, NULL);
            if (rkmessage_return != NULL) {
                values[i] = (int) rkmessage->offset;
            } else {
                //error consuming message, but partition is set
                //Not very reliable, but something...
                if (rkmessage->partition == i) {
                    if (rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
                        values[i] = (int) rkmessage->offset;
                }
            }
            rd_kafka_message_destroy(rkmessage);
        }
        *partitions = values;
        i = meta->topics->partition_cnt;
    }
    rd_kafka_metadata_destroy(meta);
    return i;
}

void kafka_consume(rd_kafka_t *r, zval* return_value, char* topic, char* offset, int item_count)
{

  int read_counter = 0;

  if (strlen(offset) != 0) {
    if (!strcmp(offset, "end"))
      start_offset = RD_KAFKA_OFFSET_END;
    else if (!strcmp(offset, "beginning"))
      start_offset = RD_KAFKA_OFFSET_BEGINNING;
    else if (!strcmp(offset, "stored"))
      start_offset = RD_KAFKA_OFFSET_STORED;
    else
      start_offset = strtoll(offset, NULL, 10);
  }
    rd_kafka_topic_t *rkt;

    if (r == NULL)
    {
        kafka_init(RD_KAFKA_CONSUMER);
        r = rk;
    }

    rd_kafka_topic_conf_t *topic_conf;

    /* Topic configuration */
    topic_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    rkt = rd_kafka_topic_new(r, topic, topic_conf);
    if (log_level) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - start_offset: %"PRId64" and offset passed: %s", start_offset, offset);

    }
    /* Start consuming */
    if (rd_kafka_consume_start(rkt, partition, start_offset) == -1) {
        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - %% Failed to start consuming: %s",
                rd_kafka_err2str(rd_kafka_errno2err(errno)));
        }
        exit(1);
    }

    if (item_count != 0) {
      read_counter = item_count;
    }

    while (run) {
      if (item_count != 0 && read_counter >= 0) {
        read_counter--;
        if (log_level) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - read_counter: %d", read_counter);
        }
        if (read_counter == -1) {
          run = 0;
          continue;//so continue, or we'll get a segfault
        }
      }

      rd_kafka_message_t *rkmessage;

      /* Consume single message.
       * See rdkafka_performance.c for high speed
       * consuming of messages. */
      rkmessage = rd_kafka_consume(rkt, partition, 1000);
      if (!rkmessage) /* timeout */
        continue;

      rd_kafka_message_t *rkmessage_return;
      rkmessage_return = msg_consume(rkmessage, NULL);
      if (rkmessage_return != NULL) {
          if ((int) rkmessage_return->len > 0) {
              //ensure there is a payload
              char payload[(int) rkmessage_return->len];
              sprintf(payload, "%.*s", (int) rkmessage_return->len, (char *) rkmessage_return->payload);
              add_index_string(return_value, (int) rkmessage_return->offset, payload, 1);
          } else {
              //add empty value
              char payload[1] = "";//empty string
              add_index_string(return_value, (int) rkmessage_return->offset, payload, 1);
          }
      }
      /* Return message to rdkafka */
      rd_kafka_message_destroy(rkmessage);
    }

    /* Stop consuming */
    rd_kafka_consume_stop(rkt, partition);
    rd_kafka_topic_destroy(rkt);
}
