<?php

namespace ingelby\rfqueue\component;


use ingelby\rfqueue\component\exceptions\RfQueueConfigurationException;
use ingelby\rfqueue\workers\WorkerInterface;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use yii\base\Component;

class RfQueue extends Component
{
    //We use this in case we cant make instance of RfQueue
    const FAIL_OVER_FILENAME_EXTENSION = '.message';

    /**
     * @var string
     */
    public $host;

    /**
     * @var string
     */
    public $port;

    /**
     * @var string
     */
    public $password;

    /**
     * @var string
     */
    public $username;

    /**
     * @var string|null
     */
    public $vhost = null;

    /**
     * @var bool
     */
    public $deadQueueEnabled = true;

    /**
     * @var string
     */
    public $deadQueueSuffix = '.dead';

    /**
     * @var array[], key is the queue name and array of booleans create queue settings, uses splat
     * ie : 'queueName' => [false, true, false, false]
     */
    public $supportedQueue = [];

    /**
     * @var string
     */
    public $queuePrefix = '';

    /**
     * @var AMQPStreamConnection
     */
    protected $connection;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @throws RfQueueConfigurationException
     */
    public function init()
    {
        parent::init();

        if (!isset(
            $this->host, $this->port, $this->password, $this->username)
        ) {
            throw new RfQueueConfigurationException('Missing configuration attribute');
        }

        try {
            $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->username, $this->password);
        } catch (\Throwable $exception) {
            throw new RfQueueConfigurationException('Unable to connect to Queue, error: ' . $exception->getMessage());
        }

        $this->channel = $this->connection->channel();

        foreach ($this->supportedQueue as $queueName => $args) {
            $this->channel->queue_declare($this->queuePrefix . $queueName, ...$args);
            if (true === $this->deadQueueEnabled) {
                $this->channel->queue_declare($this->queuePrefix . $queueName . $this->deadQueueSuffix, ...$args);
            }
        }

    }

    /**
     * @param RfQueueMessage $message
     */
    public function basicPublish(RfQueueMessage $message)
    {
        \Yii::info('Publishing to :' . $message->getTargetQueue());
        $this->channel->basic_publish(
            $message->generateAMQPMessage(),
            '',
            $this->queuePrefix . $message->getTargetQueue()
        );

        \Yii::info('Message published' . $message->getTargetQueue());
    }

    /**
     * @param string          $queueName
     * @param WorkerInterface $worker
     */
    public function basicConsume($queueName, WorkerInterface $worker)
    {
        $this->channel->basic_consume($this->queuePrefix . $queueName, '', false, false, false, false, $worker);
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel(): AMQPChannel
    {
        return $this->channel;
    }
}
