# LPE Change Data Capture Publisher (`cdc-publisher`)

This service handles the publication of Change Data Capture messages
from Oracle Advanced Queues to Apache Kafka.

## Overview

Like the [`cdc-init`] service the publisher reads Change Data Capture
Definitions from a shared Apache Kafka control topic. Any such
definitions that have a status of `active` will prompt the publisher
to monitor the corresponding queue within the IFS/Oracle database and
publish any messages it receives to the corresponding Apache Kafka
topic.

That's as simple as it gets: wait for change data capture to be
activated on something, monitor the queue, and post the messages to
Kafka.

If the publisher encounters an error that prevents it from publishing
changes related to a given Change Data Capture Definition for any
reason then it will post an updated version of the definition to the
control topic with a status of `"error"` and appropriate error details.

[`cdc-init`]: https://github.com/lymingtonprecision/cdc-init

## Usage

### Dependencies

Requires the following PL/SQL packages:

* `lpe_msg_queue_api`

Must be run under a user account with the following permissions:

```sql
create user <username> identified by <password>;
grant create session to <username>;
grant execute on ifsapp.lpe_queue_msg to <username>;
grant execute on ifsapp.lpe_msg_queue_api to <username>;
```

Generally assumes that you are creating the requisite queues,
triggers, and topics via the [`cdc-init`](../cdc-init) service.

### Environment Variables

Required:

* `DB_NAME`
* `DB_SERVER`
* `DB_USER`
* `DB_PASSWORD`
* `KAFKA_BROKERS` a Kafka [bootstrap server list][kafka-prod-conf]
  to use for establishing connections to the Kafka brokers

[kafka-prod-conf]: http://kafka.apache.org/documentation.html#producerconfigs

Optional:

* `CONTROL_TOPIC` the name of the Kafka topic from which to read/post
  requests and progress updates. Will default to `change-data-capture`.

The control topic will be created if it does not exist.

### Running

In all cases you need to first establish the environment variables as
detailed above. (Note: this project uses the [environ] library so any
supported method—`ENV` vars, `.lein-env` files, etc.—will work.)

From the project directory:

    lein run

Using a compiled `.jar` file:

    java -jar <path/to/cdc-publisher.jar>

As a [Docker] container:

    docker run \
      -d \
      --name=cdc-publisher \
      -e DB_NAME=<database> \
      -e DB_SERVER=<hostname> \
      -e DB_USER=<username> \
      -e DB_PASSWORD=<password> \
      -e KAFKA_BROKERS=<connect string> \
      lpe/cdc-publisher

[environ]: https://github.com/weavejester/environ
[Docker]: https://www.docker.com/

## Building a Docker Image

Nothing special, you just need to ensure you've built the uberjar first:

    lein unberjar
    docker build -t lymingtonprecision/cdc-publisher:latest .

[Published images] are available from [Docker Hub].

[Published images]: https://hub.docker.com/r/lymingtonprecision/cdc-publisher/
[Docker Hub]: https://hub.docker.com

## License

Copyright © 2015 Lymington Precision Engineers Co. Ltd.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
