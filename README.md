# A simple job scheduler for distributed cluster

This is a simple yet highly customizable job scheduler.

Main features:

1. The scheduler manages a list of workers. It automatically assigns jobs to idle workers.
1. Uses a user-written customizable script to manage job distribution and collection.
1. The user can add new jobs or workers on the fly.
1. The scheduler saves and loads the states of the workers and the jobs automatically. The user does not need to reconfigure the schedular every time.
1. It is just one Python source file without any dependencies.

A typical use case is distributing data files into multiple machines for data processing, and then gathering the processed files back.

## Installation

Download [cli.py](./cli.py).

## Usage

### Write a job script

A job script handles copying files between the scheduler machine and workers, preparing the environment of the workers, and running the job itself.
Your job script will be run with two environment variables: `JOB_ID` and `SLAVE_IP`. `JOB_ID` is a unique id of the job, which you can use to name temporary files. `SLAVE_IP` is the hostname (or IP address) of a worker.

Typically, a job script will contain three parts: preparing the environment, running the job, collecting the results.

```bash
#! /bin/bash
# Preparing the environment, copying necessary files etc.
rsync -aP $1.txt root@$SLAVE_IP:/root/
# Running the job
ssh -t -t root@$SLAVE_IP "wc /root/$1.txt > /root/$JOB_ID.out"
# Collecting the results
rsync -aP root@$SLAVE_IP:/root/$JOB_ID.out .
mv $JOB_ID.out $1.out
# Optionally cleaning up the remote machine
ssh -t -t root@$SLAVE_IP "rm -f /root/$1.txt /root/$JOB_ID.out"
```

Now you save it as `job_script.sh` and give it execute permission `chmod +x ./job_script.sh`.

You can test your job script by running `SLAVE_IP=xxx.xxx.xxx.xxx JOB_ID=test_job_id ./job_script.sh data_1` to see if your script produces `data_1.out` on `data_1.txt`.

An more robust (supports cancellation and handles errors) exmaple of job script is available in [example_job.sh](./example_job.sh).

### Start the server

```bash
./cli.py start [<server_data_dir>] [<log_dir>]
# server_data_dir is by default ".data"
# log_dir is by default "logs"
```

`server_data_dir` needs to be unique for each scheduler instance. And **every other command needs to specify the same `server_data_dir`**.
The scheduler works best in the background:

```bash
# Use nohup
nohup ./cli.py &
# Or use screen
screen -S scheduler
./cli.py
# Ctrl+a d to detach and screen -r scheduler to reattach.
```

### Register workers

A worker is a minimal job processing unit, which usually means a node in a cluster.

```bash
./cli.py add_slave [--skip_ssh_auth_check] <ip_1> [<ip_2>] ...
```

Example:

```bash
# This adds three workers to the list
./cli.py add_slave 192.168.1.199 192.168.1.200 192.168.1.201
```

Since you have the full control of the job script, you don't have to use IP addresses. However, it needs to be unique for each worker.
In this case, `--skip_ssh_auth_check` is required to skip the SSH no-password check.

Note: you cannot interact with your job script in the command line. So it is best to [ensure no password is needed to ssh into the worker](https://www.ssh.com/academy/ssh/copy-id).

### Add jobs

A job consists of the script name and its arguments.

```bash
./cli.py add_job <script> <args1> <args2> ...
```

Example

```bash
./cli.py add_job job_script.sh data_1
```

Each time a job is added or a worker becomes idle, the scheduler will assign a job from the waiting list to an idle worker.

If all workers are busy, the scheduler will add the job to a waiting list. The user can remove jobs in the waiting list by specifying the same script and arguments.
If multiple jobs share the same script and arguments, all of them will be removed.

```bash
./cli.py remove_job <script> <args1> <args2> ...
```

### Check the status

This prints the status of all the awaiting jobs and workers to the console.

```bash
./cli.py status
```

### Stop the server

```bash
./cli.py stop
```

Stopping the server does not kill all running jobs. It will just kill the scheduler.
The running jobs will continue to run, but there will be no more scheduling until you start the scheduler again.
