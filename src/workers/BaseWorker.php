<?php


namespace ingelby\rfqueue\workers;


use ingelby\rfqueue\component\RfQueue;
use ingelby\rfqueue\component\RfQueueMessage;
use ingelby\rfqueue\workers\exceptions\DeadQueuePublishException;
use ingelby\rfqueue\workers\exceptions\MySqlGoneAwayException;
use ingelby\rfqueue\workers\exceptions\WorkerException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Yii;
use yii\base\InvalidArgumentException;
use yii\helpers\Json;

abstract class BaseWorker implements WorkerInterface
{
    public $logCategory = 'queue';
    protected $failOverDirectory;

    /**
     * @param array $attributes
     */
    public static function selfPublish(array $attributes = []): void
    {
        \Yii::$app->rfQueue->basicPublish(
            new RfQueueMessage(
                static::queueName(),
                $attributes
            )
        );
    }

    /**
     * BaseWorker constructor.
     *
     * @param string|null $failOverDirectory
     */
    public function __construct($failOverDirectory = null)
    {
        if (null === $failOverDirectory) {
            $failOverDirectory = '/tmp';
        }
        $this->failOverDirectory = $failOverDirectory;
    }

    /**
     * @return string
     * @deprecated
     * @see static::getQueueName()
     */
    abstract public function queueName(): string;

    /**
     * @return string
     */
    abstract public static function getQueueName(): string;

    /**
     * @param array $message
     * @return mixed
     * @throws WorkerException
     */
    abstract public function run(array $message);

    /**
     * @param AMQPMessage $message
     */
    public function __invoke(AMQPMessage $message)
    {
        try {
            Yii::info('Running ' . static::class, $this->logCategory);

            try {
                $data = Json::decode($message->getBody());
            } catch (InvalidArgumentException $exception) {
                throw new WorkerException('Unable to json decode: ' . $exception->getMessage());
            }

            try {
                $this->run($data);
            } catch (\Throwable $exception) {
                if (false !== strpos($exception->getMessage(), 'SQLSTATE[HY000]')) {
                    throw new MySqlGoneAwayException('Connection to MYSQL gone away', true, 0, $exception);
                }
                throw $exception;
            }

        } catch (MySqlGoneAwayException $exception) {
            Yii::error('Unable to process message: ' . $exception->getMessage(), $this->logCategory);
            try {
                \Yii::$app->db->open();
                $this->retry($message->body);
                Yii::info('Connection reopened, message re-queued');
            } catch (\Exception  $exception) {
                Yii::error('Unable to reopen connection to database: ' . $exception->getMessage());
                $this->writeToDead($message);
            }
        } catch (WorkerException $exception) {
            Yii::error('Unable to process message: ' . $exception->getMessage(), $this->logCategory);
            $this->writeToDead($message);
        } catch (\Throwable $exception) {
            $error = [
                'message'  => $exception->getMessage(),
                'trace'    => $exception->getTraceAsString(),
                'line'     => $exception->getLine(),
                'file'     => $exception->getFile(),
                'previous' => '',
            ];

            if (null !== $exception->getPrevious()) {
                $error['previous'] = $exception->getPrevious()->getMessage();
            }

            // @Todo, Maybe set off some sort of alert
            Yii::error($error, $this->logCategory);
            $this->writeToFile($message->body);
        }


        /** @var AMQPChannel $channel */
        $channel = $message->delivery_info['channel'];
        $channel->basic_ack($message->delivery_info['delivery_tag']);

        Yii::info('Complete ' . static::class, $this->logCategory);
    }

    /**
     * @param AMQPMessage $message
     */
    protected function writeToDead($message)
    {
        try {
            $this->writeToDeadQueue($message->body);
        } catch (DeadQueuePublishException $exception) {
            Yii::error(
                'Unable to publish to dead queue, writing to file: ' . $exception->getMessage(),
                $this->logCategory
            );
            $this->writeToFile($message->body);
        }
    }

    /** @noinspection PhpDocMissingThrowsInspection */

    /**
     * @param string $body
     */
    protected function writeToFile($body)
    {
        Yii::warning('Writing queue message to file', $this->logCategory);
        $folderPath = $this->failOverDirectory;

        /** @noinspection PhpUnhandledExceptionInspection random int wont fail */
        $fileName = '/' . $this->queueName() . '_' . time() . '_' . random_int(1000, 9999) .
            RfQueue::FAIL_OVER_FILENAME_EXTENSION;

        $fullPath = $folderPath . $fileName;
        file_put_contents($fullPath, $body);
    }

    /**
     * @param string $originalMessageBody
     * @throws DeadQueuePublishException
     */
    protected function writeToDeadQueue(string $originalMessageBody)
    {
        $deadQueueName = $this->queueName() . Yii::$app->rfQueue->deadQueueSuffix;
        Yii::warning('Writing queue message to dead queue: ' . $deadQueueName, $this->logCategory);

        try {
            Yii::$app->rfQueue->basicPublish(new RfQueueMessage($deadQueueName, Json::decode($originalMessageBody)));
        } catch (\Throwable $exception) {
            Yii::warning($exception->getMessage(), $this->logCategory);
            throw new DeadQueuePublishException(
                'Unable to publish to dead queue, ' . $exception->getMessage(),
                true,
                0,
                $exception
            );
        }
    }

    /**
     * @param string $originalMessageBody
     * @throws WorkerException
     */
    public function retry(string $originalMessageBody)
    {
        Yii::info('Retrying Message');
        $queueName = $this->queueName();
        Yii::warning('Writing queue message to queue: ' . $queueName, $this->logCategory);

        try {
            Yii::$app->rfQueue->basicPublish(new RfQueueMessage($queueName, Json::decode($originalMessageBody)));
            Yii::info('Retrying successful');
        } catch (\Throwable $exception) {
            Yii::warning($exception->getMessage(), $this->logCategory);
            throw new WorkerException(
                'Unable to republish to retry queue, ' . $exception->getMessage(),
                true,
                0,
                $exception
            );
        }
    }
}
