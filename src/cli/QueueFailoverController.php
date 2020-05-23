<?php

namespace ingelby\rfqueue\cli;

use ingelby\rfqueue\component\RfQueue;
use ingelby\rfqueue\component\RfQueueMessage;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use yii\console\Controller;
use yii\helpers\Console;
use yii\helpers\Json;

class QueueFailoverController extends Controller
{

    /**
     * Will attempt to process a directory of files
     *
     * @param string|null $folderPath will default to failed messages directory
     */
    public function actionReplayFailedMessages($failedFolderPath)
    {
        \Yii::info('Processing files in path: ' . $failedFolderPath);
        if (!file_exists($failedFolderPath)) {
            throw new \RuntimeException($failedFolderPath . ' directory does not exist');
        }

        $files = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($failedFolderPath),
            RecursiveIteratorIterator::SELF_FIRST
        );

        $filesProcessed = 0;
        foreach ($files as $file) {
            $this->processFile($file);
            $filesProcessed++;
        }

        \Yii::info('Proceeded ' . $filesProcessed . ' files');
    }


    /**
     * @param string $filePath
     */
    protected function processFile($filePath)
    {
        if (\in_array(substr($filePath, strrpos($filePath, '/') + 1), ['.', '..'])) {
            return;
        }

        if (true !== is_file($filePath)) {
            \Yii::info($filePath . ' is not a file, skipping');
            return;
        }

        $filePath = realpath($filePath);
        $pathParts = pathinfo($filePath);
        if (substr(RfQueue::FAIL_OVER_FILENAME_EXTENSION, 1) !== $pathParts['extension']) {
            \Yii::info($filePath . ' is has not got correct extension of: ' . RfQueue::FAIL_OVER_FILENAME_EXTENSION);
            return;
        }

        $filePathName = basename($filePath, RfQueue::FAIL_OVER_FILENAME_EXTENSION);

        $queueName = explode('_', $filePathName);

        if (empty($queueName)) {
            \Yii::warning('Issue with name of file, likely not to be correct format so wrong queue name');
            return;
        }

        $queueName = $queueName[0];

        $payload = file_get_contents($filePath);

        try {
            $data = Json::decode($payload);
            Console::output($queueName);
            Console::output($payload);
            \Yii::$app->rfQueue->basicPublish(new RfQueueMessage($queueName, $data));
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

            \Yii::error($error);
            return;
        }
        \Yii::info($filePath . ' published to queue, deleting file');

        $success = unlink($filePath);

        if (false === $success) {
            \Yii::error('Unable to delete file at path: ', $filePath);
        }
    }

    /**
     * Will attempt to process a failover file
     *
     * @param string $pathToMessage
     */
    public function actionReplaySingleFile($pathToMessage)
    {
        \Yii::info('Processing file in path: ' . $pathToMessage);
        if (!file_exists($pathToMessage)) {
            throw new \RuntimeException($pathToMessage . ' does not exist');
        }

        $this->processFile($pathToMessage);
        \Yii::info('Finished');

    }
}
