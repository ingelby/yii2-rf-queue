<?php

namespace ingelby\rfqueue\workers;

use PhpAmqpLib\Message\AMQPMessage;

interface WorkerInterface
{
    public function __invoke(AMQPMessage $message);
    public function run(array $message);
}