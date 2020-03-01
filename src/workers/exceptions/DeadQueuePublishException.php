<?php


namespace ingelby\rfqueue\workers\exceptions;


use Throwable;
use yii\base\Exception;

class DeadQueuePublishException extends Exception
{
    /**
     * @var bool
     */
    protected $shouldWriteToFile;

    public function __construct($message = "", bool $writeToFile = true, $code = 0, Throwable $previous = null)
    {
        $this->shouldWriteToFile = $writeToFile;
        parent::__construct($message, $code, $previous);
    }

    /**
     * @return bool
     */
    public function shouldWriteToFile(): bool
    {
        return $this->shouldWriteToFile;
    }
}
