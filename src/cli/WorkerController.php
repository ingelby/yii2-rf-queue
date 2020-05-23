<?php

namespace ingelby\rfqueue\cli;


use yii\base\BootstrapInterface;
use yii\console\Application as ConsoleApp;
use yii\console\Controller;
use yii\console\Exception;

class WorkerController extends Controller implements BootstrapInterface
{
    /**
     * @inheritdoc
     */
    public function bootstrap($app)
    {
        if ($app instanceof ConsoleApp) {
            $app->controllerMap[$this->getCommandId()] = [
                    'class' => $this->commandClass,
                    'queue' => $this,
                ] + $this->commandOptions;
        }
    }

    /**
     * @param string      $queueName
     * @param string      $worker fully qualifying class name
     * @param int         $prefetch
     * @param string|null $failoverDirectory full path of the failover directory
     * @throws Exception
     * @throws \ErrorException
     */
    public function actionListen($queueName, $worker, $prefetch = 10, $failoverDirectory = null)
    {
        \Yii::info('Starting worker: ' . $worker . ' listening to queue: ' . $queueName);

        if (null !== $failoverDirectory && !is_dir($failoverDirectory)) {
            throw new Exception($failoverDirectory . ' is not a valid directory');
        }

        if (!class_exists($worker)) {
            throw new Exception($worker . ' doest not exist');
        }

        $channel = \Yii::$app->rfQueue->getChannel();
        $channel->basic_qos(null, $prefetch, null);
        \Yii::$app->rfQueue->basicConsume($queueName, new $worker($failoverDirectory));

        while ($channel->is_consuming()) {
            $channel->wait();
        }
    }
}
