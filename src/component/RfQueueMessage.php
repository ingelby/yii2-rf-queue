<?php

namespace ingelby\rfqueue\component;

use PhpAmqpLib\Message\AMQPMessage;
use yii\helpers\Json;

class RfQueueMessage
{
    /**
     * @var string
     */
    protected $targetQueue;

    /**
     * @var array
     */
    protected $data;

    /**
     * @var string
     */
    protected $deliveryMode = AMQPMessage::DELIVERY_MODE_PERSISTENT;

    public function __construct($targetQueue, array $data, $deliveryMode = null)
    {
        $this->targetQueue = $targetQueue;
        $this->data = $data;
        if (null !== $deliveryMode) {
            $this->deliveryMode = $deliveryMode;
        }
    }

    /**
     * @return AMQPMessage
     */
    public function generateAMQPMessage()
    {
        return new AMQPMessage(
            Json::encode($this->data),
            ['delivery_mode' => $this->deliveryMode]
        );
    }

    /**
     * @return string
     */
    public function getTargetQueue(): string
    {
        return $this->targetQueue;
    }

    /**
     * @return array
     */
    public function getData(): array
    {
        return $this->data;
    }

    /**
     * @return string
     */
    public function getDeliveryMode(): string
    {
        return $this->deliveryMode;
    }

}