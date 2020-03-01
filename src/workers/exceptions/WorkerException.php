<?php

namespace ingelby\rfqueue\workers\exceptions;

use Throwable;
use yii\base\Exception;

class WorkerException extends Exception
{
    /**
     * @var bool
     */
    protected $shouldRequeue;

    public function __construct($message = "", bool $requeue = true, $code = 0, Throwable $previous = null)
    {
        $this->shouldRequeue = $requeue;
        parent::__construct($message, $code, $previous);
    }

    /**
     * @return bool
     */
    public function shouldRequeue(): bool
    {
        return $this->shouldRequeue;
    }
}
