<?php

namespace sigalx\amqpio;

class AmqpIoException extends \Exception
{
}

class AmqpIoExchange
{
    /** @var AmqpIo */
    protected $_amqp;
    /** @var \AMQPExchange */
    protected $_internal;

    public function __construct(AmqpIo $amqpIo, \AMQPExchange $internal)
    {
        $this->_amqp = $amqpIo;
        $this->_internal = $internal;
    }

    /**
     * @param mixed $data
     * @param string $subroute
     * @param int $flags
     * @param array $attributes
     * @throws AmqpIoException
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function sendMessage($data, string $subroute, int $flags = AMQP_NOPARAM, array $attributes = []): void
    {
        if (is_array($data) || is_object($data)) {
            $data = @json_encode($data, JSON_THROW_ON_ERROR);
        }
        if (!$this->_internal->publish($data, $this->_amqp->makeRouteName($subroute), $flags, $attributes)) {
            throw new AmqpIoException('Cannot publish a message into AMQP exchange');
        }
    }

    public function getName(): string
    {
        return $this->_internal->getName();
    }
}

class AmqpIoQueue
{
    /** @var AmqpIo */
    protected $_amqp;
    /** @var \AMQPQueue */
    protected $_internal;

    public function __construct(AmqpIo $amqpIo, \AMQPQueue $internal)
    {
        $this->_amqp = $amqpIo;
        $this->_internal = $internal;
    }

    /**
     * @param string $exchangeName
     * @param string $subroute
     * @return AmqpIoQueue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function bind(string $exchangeName, string $subroute): AmqpIoQueue
    {
        $this->_internal->bind($exchangeName, $this->_amqp->makeRouteName($subroute));
        return $this;
    }

    /**
     * @param string $subroute
     * @return AmqpIoQueue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function bindDirect(string $subroute): AmqpIoQueue
    {
        $this->bind('amq.direct', $subroute);
        return $this;
    }

    /**
     * @param string $subroute
     * @return AmqpIoQueue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function bindTopic(string $subroute): AmqpIoQueue
    {
        $this->bind('amq.topic', $subroute);
        return $this;
    }

    /**
     * @param string $subroute
     * @return AmqpIoQueue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function bindFanout(string $subroute): AmqpIoQueue
    {
        $this->bind('amq.fanout', $subroute);
        return $this;
    }

    public function getInternal(): \AMQPQueue
    {
        return $this->_internal;
    }
}

class AmqpIo
{
    /** @var AmqpIo */
    private static $_instance;

    /**
     * Used in global single-instance-style
     * @var string
     */
    public static $instanceName = 'unknown';

    /**
     * Used in global single-instance-style
     * @var array
     */
    public static $credentials = [];

    /** @var string */
    protected $_instanceName;
    /** @var \AMQPConnection */
    protected $_amqpConnection;
    /** @var \AMQPChannel */
    protected $_amqpChannel;
    /** @var AmqpIoExchange[] */
    protected $_exchanges;

    /**
     * For global single-instance-style using
     * @return AmqpIo
     * @throws \AMQPConnectionException
     */
    public static function instance(): AmqpIo
    {
        if (!isset(self::$_instance)) {
            self::$_instance = new static(static::$instanceName, static::$credentials);
        }
        return self::$_instance;
    }

    /**
     * @param string $instanceName
     * @param array $credentials
     * @throws \AMQPConnectionException
     */
    public function __construct(string $instanceName, array $credentials = [])
    {
        $this->connect($credentials);
        $this->_instanceName = $instanceName;
    }

    private function __clone()
    {
    }

    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Pass the array optionally containing following keys:
     *     host, port, vhost, login, password, read_timeout, write_timeout, connect_timeout
     *
     * @param array $credentials
     * @throws \AMQPConnectionException
     */
    public function connect(array $credentials = [])
    {
        if ($this->isConnected()) {
            return;
        }
        $this->_amqpConnection = new \AMQPConnection($credentials);
        $this->_amqpConnection->connect();
        $this->_amqpChannel = new \AMQPChannel($this->_amqpConnection);
    }

    public function disconnect()
    {
        if ($this->isConnected()) {
            $this->_amqpConnection->disconnect();
        }
    }

    public function isConnected(): bool
    {
        return $this->_amqpConnection && $this->_amqpConnection->isConnected();
    }

    /**
     * @param string $name
     * @param string $type
     * @param int $flags
     * @return AmqpIoExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function getExchange(string $name, string $type, int $flags = AMQP_DURABLE): AmqpIoExchange
    {
        $this->connect();
        if (!isset($this->_exchanges[$name])) {
            $exchange = new \AMQPExchange($this->_amqpChannel);
            $exchange->setName($name);
            $exchange->setType($type);
            $exchange->setFlags($flags);
            $this->_exchanges[$name] = new AmqpIoExchange($this, $exchange);
        }
        return $this->_exchanges[$name];
    }

    /**
     * @return AmqpIoExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function getExchangeDirect(): AmqpIoExchange
    {
        return $this->getExchange('amq.direct', AMQP_EX_TYPE_DIRECT, AMQP_DURABLE);
    }

    /**
     * @return AmqpIoExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function getExchangeTopic(): AmqpIoExchange
    {
        return $this->getExchange('amq.topic', AMQP_EX_TYPE_TOPIC, AMQP_DURABLE);
    }

    /**
     * @return AmqpIoExchange
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function getExchangeFanout(): AmqpIoExchange
    {
        return $this->getExchange('amq.fanout', AMQP_EX_TYPE_FANOUT, AMQP_DURABLE);
    }

    public function makeRouteName(string $subroute): string
    {
        return "{$this->_instanceName}.{$subroute}";
    }

    public function makeQueueName(string $queueName): string
    {
        return "{$this->_instanceName}.{$queueName}";
    }

    /**
     * @param string $queueName
     * @param int $queueFlags
     * @return AmqpIoQueue
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function initQueue(string $queueName, int $queueFlags = AMQP_NOPARAM): AmqpIoQueue
    {
        $this->connect();
        $queueName = static::makeQueueName($queueName);
        $amqpQueue = new \AMQPQueue($this->_amqpChannel);
        $amqpQueue->setName($queueName);
        $amqpQueue->setFlags($queueFlags);
        $amqpQueue->declareQueue();
        return new AmqpIoQueue($this, $amqpQueue);
    }
}
