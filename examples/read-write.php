#!/usr/bin/env php
<?php

require_once(__DIR__ . '/../vendor/autoload.php');

\sigalx\amqpio\AmqpIo::$instanceName = 'amqpio-example';

/** @noinspection PhpUnhandledExceptionInspection */
$queue = \sigalx\amqpio\AmqpIo::AmqpIo()
    ->initQueue('example-queue')
    ->bindDirect('example-route');

/** @noinspection PhpUnhandledExceptionInspection */
\sigalx\amqpio\AmqpIo::AmqpIo()
    ->getExchangeDirect()
    ->sendMessage('example-data', 'example-route');

/** @noinspection PhpUnhandledExceptionInspection */
$queue->getInternal()->consume(function (AMQPEnvelope $envelope, AMQPQueue $queue): bool {
    echo "We got {$envelope->getBody()}\n";
    $queue->ack($envelope->getDeliveryTag());
    return false;
});
