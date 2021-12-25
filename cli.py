#! /usr/bin/python3
import sys
import subprocess
import os
import logging
import json
from collections import deque
from typing import List
import time
import threading
import pickle
from argparse import ArgumentParser


class ServerContext:
    """
    Stores all persistable and non-persistable server states.
    Persistable states should be saved using `_save` method.
    """

    def __init__(self, data_dir: str, logs_dir: str) -> None:
        self.slaves = []  # type: List[Slave]
        self.job_waitlist = deque()  # type: deque
        self.lock = threading.RLock()
        self.should_stop = False
        self.logs_dir = logs_dir
        self._server_context_file = os.path.join(data_dir, "server_context.pkl")
        self._load()

    def add_job(self, args: List[str]):
        """
        Adds a job to wait list

        Args:
            args (List[str]): The name of a shell script and its parameters.

            E.g. ["job.sh", "data.txt", "data2.txt"] will be run as "./job.sh data.txt data2.txt".
        """
        with self.lock:
            self.job_waitlist.put(args)
            self._save()
            self.schedule()

    def add_slave(self, ip: str):
        """Register a slave

        Args:
            ip (str): The IP address of that slave. Make sure it does not require password authentication when SSH into it.
        """
        with self.lock:
            self.slaves.append(Slave(ip, self))
            self._save()
            self.schedule()

    def _save(self):
        with self.lock, open(self._server_context_file, "wb") as out_file:
            pickle.dump((self.slaves, self.job_waitlist), out_file)

    def _load(self):
        with self.lock:
            if os.path.exists(self._server_context_file):
                with open(self._server_context_file, "rb") as in_file:
                    self.slaves, self.job_waitlist = pickle.load(in_file)
                for slave in self.slaves:
                    slave.associate(self)

    def shutdown(self):
        self._save()

    def schedule(self):
        """Schedule waiting jobs to idle slaves.
        Be careful this method will be called from different threads.
        """
        with self.lock:
            while len(self.job_waitlist) > 0:
                hasIdleSlave = False
                for slave in self.slaves:
                    if slave.status == "idle":
                        hasIdleSlave = True
                        job = self.job_waitlist.popleft()
                        slave.run(job)
                if not hasIdleSlave:
                    break

    def to_JSON(self):
        return json.dumps({
            "job_waitlist": list(self.job_waitlist),
            "slaves": [{
                "ip": slave.ip,
                "status": slave.status,
                "running_job": slave.running_job,
            } for slave in self.slaves]
        })


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


class JobInfo:
    def __init__(self, id: str, script: str, pid: int, log_file: str) -> None:
        self.id = id
        self.script = script
        self.pid = pid
        self.log_file = log_file

    def is_alive(self):
        return check_pid(self.pid)

    def __str__(self) -> str:
        alive_status = "Alive" if self.is_alive() else "Dead"

        return f"""Job {self.id}: {alive_status}
    Script: {self.script}
    Last 10 lines of {self.log_file}:
    {subprocess.run(f"tail -n 10 {self.log_file}", shell=True).stdout.decode("utf-8")}
"""


class Slave:
    def __init__(self, ip, ctx: ServerContext) -> None:
        self.ip = ip
        self.status = "idle"
        self.running_job = None  # type: JobInfo
        self.ctx = ctx
        self.process_waiter = None
        self._shutdown_event = threading.Event()

    def run(self, job_script: List[str]):
        """Assign a job to this idle slave

        Args:
            job_script (List[str]): The job script's name and its parameters
        """
        if self.status != "idle":
            logging.error("The slave is not idle")
            return
        self.status = "busy"
        job_id = str(int(time.time() * 1000))
        logging.info(
            f"Running job {job_id} on {self.ip} > {self.ctx.logs_dir}/job_{job_id}")
        logging.info(" ".join(job_script))
        t = threading.Thread(name=f"Worker {self.ip} {job_id}", target=_worker, args=[
                             job_id, job_script, self])
        t.start()
        logging.info(
            f"Finished job {job_id} on {self.ip} > {self.ctx.logs_dir}/job_{job_id}")

    def monitor(self):
        """Start a monitoring thread if there is a running job.
        The monitering thread will reset the slave's states Once the job is finished.
        """
        if self.running_job is not None and self.process_waiter is None:
            pid = self.running_job.pid
            self.process_waiter = threading.Thread(
                name=f"Job waiter {self.ip} {self.running_job.id}",
                target=_process_waiter, args=[pid, self])
            self.process_waiter.start()

    def shutdown(self):
        if self.process_waiter is not None:
            self._shutdown_event.set()
            self.process_waiter.join()

    def __getstate__(self):
        return (self.ip, self.status, self.running_job)

    def __setstate__(self, state):
        """Make sure to call Slave.associate afterwards"""
        self.ip, self.status, self.running_job = state

    def associate(self, ctx: ServerContext):
        """Associate this slave to a server context object. Only used after loading the server context object from file.

        Args:
            ctx (ServerContext): The Server context object.
        """
        self.process_waiter = None
        self.ctx = ctx
        self.monitor()


def _process_waiter(pid, slave: Slave):
    while check_pid(pid):
        if slave._shutdown_event.wait(1):
            # slave is shutting down, keep its inner states intact for serialization later
            return

    slave.status = "idle"
    slave.process_waiter = None
    slave.running_job = None
    slave.ctx.schedule()


def _worker(job_id, job_script, slave: Slave):
    subprocess.run("mkdir -p jobs", shell=True)
    log_file = os.path.join("jobs", f"job_{job_id}.txt")
    with open(log_file, "wb") as out:
        proc = subprocess.Popen(job_script, stdout=out, stderr=subprocess.STDOUT, env={
            "JOB_ID": job_id,
            "SLAVE_IP": slave.ip
        }, start_new_session=True)
        slave.running_job = JobInfo(job_id, job_script, proc.pid, log_file)
        slave.monitor()


def start(base_parser):
    parser = ArgumentParser(
        description="Start the scheduler service.", parents=[base_parser])
    parser.add_argument("--log_dir", type=str, default="logs")
    args = parser.parse_args()
    data_dir = args.server_data_dir
    logs_dir = args.log_dir
    subprocess.run(f"mkdir -p {data_dir}", shell=True)
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    pid_file_path = os.path.join(data_dir, "service_pid")

    def ensure_fifo():
        if not os.path.exists(commands_fifo_path):
            subprocess.run(f"mkfifo {commands_fifo_path}", shell=True)

    if os.path.exists(commands_fifo_path):
        try:
            with open(pid_file_path, "r") as f:
                last_pid = int(f.read())
            if check_pid(last_pid):
                logging.error(
                    "`commands_fifo` already exists in this folder. Try shutting down the already running server with `python3 cli.py stop`")
                sys.exit(-1)
            subprocess.run(f"rm -f {commands_fifo_path}", shell=True)
        except:
            pass

    with open(pid_file_path, "w") as f:
        f.write(str(os.getpid()))
    logging.info(f"Handling existing tasks")
    logging.info(f"Server started")

    ctx = ServerContext(logs_dir)

    ensure_fifo()
    threading.Thread(name="print status", target=status, args=[base_parser]).start()

    while True:
        ensure_fifo()
        with open(commands_fifo_path, "r", encoding="utf-8") as f:
            try:
                cmd = json.load(f)
            except:
                logging.error("Error parsing json from stream")
                continue
            if cmd['type'] == 'shutdown':
                break
            elif cmd['type'] == 'addjob':
                ctx.add_job(cmd['args'])
            elif cmd['type'] == 'addslave':
                ctx.add_slave(cmd['ip'])
            elif cmd['type'] == 'status':
                fifo_name = cmd['pipe']
                with open(fifo_name, "w", encoding="utf-8") as f:
                    f.write(ctx.to_JSON() + "\n")
            else:
                logging.error(f"Unknown command type {cmd['type']}")
    logging.info(f"Shutting down")
    ctx.shutdown()


def stop(parser: ArgumentParser):
    args = parser.parse_args()
    data_dir = args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    with open(commands_fifo_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "type": "shutdown"
        }) + "\n")


def add_job(parser: ArgumentParser):
    args, rest_args = parser.parse_known_args()
    data_dir = args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    if len(rest_args) == 0:
        logging.error("Usage: addjob <template.sh> [arg1] [arg2] ...")
        sys.exit(-1)

    if not os.path.isfile(rest_args[0]):
        logging.error(f"{rest_args[0]} does not exist")
        sys.exit(-1)

    if not rest_args[0].startswith("./"):
        rest_args[0] = "./" + rest_args[0]

    with open("commands_fifo", "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "type": "addjob",
            "args": rest_args
        }) + "\n")


def add_slave(parser: ArgumentParser):
    parser = ArgumentParser(description="Add a slave.", parents=[parser])
    parser.add_argument("ip", type=str, nargs='+',
                        required=True, help="IP addresses of the slaves")
    args = parser.parse_args()
    data_dir = args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    with open(commands_fifo_path, "w", encoding="utf-8") as f:
        for ip in args.ip:
            p = subprocess.run(
                f"ssh -o PasswordAuthentication=no {ip} /bin/true", shell=True)
            if p.returncode != 0:
                logging.error(
                    f"Password login is still required for ssh {ip}. Please ensure no password is needed to ssh into {ip}.")
                sys.exit(-1)
            f.write(json.dumps({
                "type": "addslave",
                "ip": ip
            }) + "\n")


def status(parser: ArgumentParser):
    args = parser.parse_args()
    data_dir = args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    id = str(int(time.time() * 1000))
    fifo_name = f"tmp_{id}"
    subprocess.run(f"mkfifo {fifo_name}", shell=True)
    with open(commands_fifo_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({
            "type": "status",
            "pipe": fifo_name
        }) + "\n")
    with open(fifo_name, "r", encoding="utf-8") as f:
        status = json.loads(f.readline())
    subprocess.run(f"rm -f {fifo_name}", shell=True)
    print(json.dumps(status, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Scheduler service command line interface.", add_help=False)
    parser.add_argument("action", type=str, default="status",
                        help="One of start/stop/status/addjob/addslave")
    parser.add_argument("--server_data_dir", type=str, default=".data")
    
    main_parser = ArgumentParser(parents=[parser])

    args = main_parser.parse_args()
    action = args.action
    logging.basicConfig(level=logging.INFO)
    if action == "status":
        status(parser)
    elif action == "start":
        start(parser)
    elif action == "stop":
        stop(parser)
    elif action == "addjob":
        add_job(parser)
    elif action == "addslave":
        add_slave(parser)
    else:
        parser.print_help()
