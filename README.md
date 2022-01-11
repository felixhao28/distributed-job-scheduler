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
./cli.py start [--server_data_dir=<server_data_dir>] [--log_dir=<log_dir>]
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
./cli.py add_slave [--server_data_dir=<server_data_dir>] [--skip_ssh_auth_check] [--env,-e=<NAME>=<VALUE>] <ip_1> [<ip_2>] ...
```

Example:

```bash
# This adds three workers to the list
./cli.py add_slave 192.168.1.199 192.168.1.200 192.168.1.201
```

Since you have the full control of the job script, you don't have to use IP addresses. However, it needs to be unique for each worker.
In this case, `--skip_ssh_auth_check` is required to skip the SSH no-password check. For example, you can totally write something like this:

```bash
# This adds three workers to the list
./cli.py add_slave user1@199:1000 user2@200:1001 user3@201:1002
```

And use them in the script like:

```bash
# SLAVE_IP=user1@199:1000
# Now use some shell magic to extract propriate parts
USER=${SLAVE_IP%%@*} # user1
PORT=${SLAVE_IP##*:} # 1000
IP=${SLAVE_IP:${#USER} + 1: ${#PORT} - 1} # 199
ssh -t -t $USER@192.168.1.$IP -p $PORT "wc /root/$1.txt > /root/$JOB_ID.out"
```

A probably more readable way of doing this is to use `--env` argument:

```bash
# This adds three workers to the list
./cli.py add_slave -e USER=user1 -e PORT=1000 199
./cli.py add_slave -e USER=user2 -e PORT=1001 200
./cli.py add_slave -e USER=user3 -e PORT=1002 201

echo job.sh
# ssh -t -t $USER@192.168.1.$SLAVE_IP -p $PORT "wc /root/$1.txt > /root/$JOB_ID.out"
```

Or add the workers to the SSH configuration file (~/.ssh/config):

```
Host 199
    HostName 192.168.1.199
    Port 1000
    User user1
Host 200
    HostName 192.168.1.200
    Port 1001
    User user2
Host 201
    HostName 192.168.1.201
    Port 1002
    User user3
```

```bash
./cli.py add_slave 199 200 201

echo job.sh
# ssh -t -t $SLAVE_IP "wc /root/$1.txt > /root/$JOB_ID.out"
```

Note: you cannot interact with your job script in the command line. So it is best to [ensure no password is needed to ssh into the worker](https://www.ssh.com/academy/ssh/copy-id).

You can remove workers with this command:

```bash
./cli.py remove_slave [--wait|kill] <ip_1> [<ip_2>] ...
```

If the worker is busy, `--wait` will postpone the removal until the job is finished. `--kill` in constrast will kill the running job and remove the worker immediately.
It is your responsibility to handle the `SIGTERM` signal in your script to stop the job completely. A such example is [example_job.sh](./example_job.sh).

### Add jobs

A job consists of the script name and its arguments.

```bash
./cli.py add_job [--server_data_dir=<server_data_dir>] [--env,-e=<NAME>=<VALUE>] <script> <args1> <args2> ...
```

Example

```bash
./cli.py add_job job_script.sh data_1
```

Each time a job is added or a worker becomes idle, the scheduler will assign a job from the waiting list to an idle worker.
If all workers are busy, the scheduler will add the job to a waiting list.
The user can use `--env` to specify additional environment variables for the script. `--env` can be used multiple times.

The script does not need to be a shell script. Any executable file works.

In the case of an argument passed to your script have the conflicting name with `--server_data_dir`.

The user can remove jobs in the waiting list by specifying the same script and arguments.
If multiple jobs share the same script and arguments, all of them will be removed.

```bash
./cli.py remove_job [--server_data_dir=<server_data_dir>] <script> <args1> <args2> ...
```

### Check/load the status

This prints the status of all the awaiting jobs and workers to the console.

```bash
./cli.py [--server_data_dir=<server_data_dir>] status
```

It prints a json object like this:

```json
{
    "job_waitlist": [
        ["example_job.sh", "file3"]
        ["example_job.sh", "file4"]
    ],
    "slaves": [
        {
            "ip": "111.111.111.111",
            "status": "busy",
            "running_job": {
                "id": "1640569405834",
                "script": [
                    "./example_job.sh",
                    "file1"
                ],
                "pid": 26765,
                "log_file": "logs/job_1640569405834.txt"
            }
        },
        {
            "ip": "111.111.111.112",
            "status": "busy",
            "running_job": {
                "id": "1640601291867",
                "script": [
                    "./example_job.sh",
                    "file2"
                ],
                "pid": 26767,
                "log_file": "logs/job_1640601291867.txt"
            }
        }
    ]
}
```

In case of needing manually editing the status of the scheduler, save the above json into a file, edit it, and then load it with `load_status` action.

```bash
./cli.py [--server_data_dir=<server_data_dir>] load_status </path/to/status_json_file>
```

Note: this will erase the existing status of the scheduler.

### Stop the server

```bash
./cli.py [--server_data_dir=<server_data_dir>] stop
```

Stopping the server does not kill all running jobs. It will just kill the scheduler.
The running jobs will continue to run, but there will be no more scheduling until you start the scheduler again.

## Troubleshooting

If the scheduler is not behaving properly. You can reset the scheduler by deleting the `<server_data_dir>` folder (`.data` by default) and restarting the scheduler.
