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
     * @param string $queueName
     * @param string $worker fully qualifying class name
     * @param int    $prefetch
     * @throws Exception
     * @throws \ErrorException
     */
    public function actionListen($queueName, $worker, $prefetch = 10)
    {
        \Yii::info('Starting worker: ' . $worker . ' listening to queue: ' . $queueName);

        if (!class_exists($worker)) {
            throw new Exception($worker . ' doest not exist');
        }

        $channel = \Yii::$app->rfQueue->getChannel();
        $channel->basic_qos(null, $prefetch, null);
        \Yii::$app->rfQueue->basicConsume($queueName, new $worker());

        while ($channel->is_consuming()) {
            $channel->wait();
        }
    }
}
