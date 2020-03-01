<?php


namespace ingelby\rfqueue\workers;


use ingelby\rfqueue\workers\exceptions\WorkerException;

class TestWorker extends BaseWorker
{
    const QUEUE_NAME = 'test';

    /**
     * @return string
     */
    public function queueName(): string
    {
        return static::QUEUE_NAME;
    }

    /**
     * @param array $message
     * @return mixed
     * @throws WorkerException
     */
    public function run(array $message)
    {
        echo ' [x] Received "\n"';
        echo " [x] Done\n";
    }
}
