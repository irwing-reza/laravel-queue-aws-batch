<?php

/**
 * Laravel Queue for AWS Batch.
 *
 * @author    Luke Waite <lwaite@gmail.com>
 * @copyright 2017 Luke Waite
 * @license   http://www.opensource.org/licenses/mit-license.php MIT
 *
 * @link      https://github.com/lukewaite/laravel-queue-aws-batch
 */

namespace LukeWaite\LaravelQueueAwsBatch\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Cache\Repository as Cache;
use Illuminate\Foundation\Exceptions\Handler;
use Illuminate\Queue\Console\WorkCommand;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Worker;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Facades\Log;
use LukeWaite\LaravelQueueAwsBatch\Exceptions\JobNotFoundException;
use LukeWaite\LaravelQueueAwsBatch\Exceptions\UnsupportedException;
use LukeWaite\LaravelQueueAwsBatch\Queues\BatchQueue;
use Symfony\Component\Debug\Exception\FatalThrowableError;

class QueueWorkBatchCommand extends WorkCommand
{
    protected $name = 'queue:work-batch';

    protected $description = 'Run a Job for the AWS Batch queue';

    protected $signature = 'queue:work-batch
                            {job_id : The job id in the database}
                            {connection? : The name of the queue connection to work}
                            {--name=default : The name of the worker [default: "default"]}
                            {--memory=128 : The memory limit in megabytes}
                            {--timeout=60 : The number of seconds a child process can run}
                            {--tries=0 : Number of times to attempt a job before logging it failed}
                            {--force : Force the worker to run even in maintenance mode}
                            {--queue= : The names of the queues to work}
                            {--once : Only process the next job on the queue}
                            {--stop-when-empty : Stop when the queue is empty}';


    protected $manager;
    protected $exceptions;

    public function __construct(QueueManager $manager, Worker $worker, Handler $exceptions, Cache $cache)
    {
        parent::__construct($worker, $cache);
        $this->manager = $manager;
        $this->exceptions = $exceptions;
    }

    public function handle()
    {
        $this->listenForEvents();

        try {
            $this->runJob();
        } catch (\Throwable $e) {
            $this->exceptions->report($e);
            throw $e;
        }
    }

    // TOOD: Refactor out the logic here into an extension of the Worker class
    protected function runJob()
    {
        $connectionName = $this->argument('connection');
        $jobId = $this->argument('job_id');

        /** @var BatchQueue $connection */
        $connection = $this->manager->connection($connectionName);

        Log::debug("Connection Instance of", [
            'connection' => get_class($connection)
        ]);

        if (!$connection instanceof BatchQueue) {
            throw new UnsupportedException('queue:work-batch can only be run on batch queues');
        }

        $job = $connection->getJobById($jobId);

        // If we're able to pull a job off of the stack, we will process it and
        // then immediately return back out.
        if (!is_null($job)) {
            $this->worker->process(
                $this->manager->getName($connectionName),
                $job,
                $this->gatherWorkerOptions()
            );
            return;
        }

        // If we hit this point, we haven't processed our job
        throw new JobNotFoundException('No job was returned');
    }

    /**
     * Gather all of the queue worker options as a single object.
     *
     * @return \Illuminate\Queue\WorkerOptions
     */
    protected function gatherWorkerOptions()
    {
        return new WorkerOptions(
            'default', // name
            0, //backoff delay
            $this->option('memory'),
            $this->option('timeout'),
            0,
            $this->option('tries'),
            false,
            $this->option('stop-when-empty'),
        );

        //$name = 'default', $backoff = 0, $memory = 128, $timeout = 60, $sleep = 3, $maxTries = 1,
        //$force = false, $stopWhenEmpty = false, $maxJobs = 0, $maxTime = 0, $rest = 0
    }
}
