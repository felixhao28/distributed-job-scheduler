#!/usr/bin/env python3
import argparse
import sys
import subprocess
import os
import logging
import json
from collections import deque
from typing import Dict, List, Tuple
import time
import threading
import pickle
import traceback
from argparse import ArgumentParser


def dumper(obj):
    try:
        return obj.toJSON()
    except:
        return obj.__dict__


class ServerContext:
    """
    Stores all persistable and non-persistable server states.
    Persistable states should be saved using `_save` method.
    """

    def __init__(self, data_dir: str, logs_dir: str) -> None:
        self.slaves = []  # type: List[Slave]
        self.job_waitlist = deque()  # type: deque[Tuple[List[str], List[str]]]
        self.lock = threading.RLock()
        self.should_stop = False
        self.logs_dir = logs_dir
        self._server_context_file = os.path.join(
            data_dir, "server_context.pkl")
        self._load()

    def add_job(self, args: List[str], envs: List[str]):
        """
        Adds a job to wait list

        Args:
            args (List[str]): The name of a shell script and its parameters.

            E.g. ["job.sh", "data.txt", "data2.txt"] will be run as "./job.sh data.txt data2.txt".
        """
        with self.lock:
            self.job_waitlist.append((args, envs))
            self._save()
            slave = self.schedule()
            if len(self.job_waitlist) > 0:
                return {"msg": "All slaves are busy. Job is added to the waiting list."}
            elif slave is not None:
                return {"msg": f"Job is asigned to {slave.ip}."}
            else:
                return {"msg": "ServerContext.add_job: This should never be reached."}

    def remove_job(self, args: List[str], envs: List[str]):
        with self.lock:
            try:
                self.job_waitlist.remove((args, envs))
                self._save()
                return {"msg": "The job is removed from waitlist"}
            except Exception as e:
                return {"msg": f"Failed to remove the job from waitlist: {str(e)}"}

    def add_slave(self, ip: str, envs: List[str]):
        """Register a slave

        Args:
            ip (str): The IP address of that slave. Make sure it does not require password authentication when SSH into it.
        """
        with self.lock:
            for slave in self.slaves:
                if slave.ip == ip:
                    return {"err": f"{ip} is already added"}
            self.slaves.append(Slave(ip, envs, self))
            self._save()
            self.schedule()
        return {"msg": "ok"}

    def remove_slave(self, ip: str, options: dict):
        """Remove a slave

        Args:
            ip (str): The IP address of that slave.
        """
        with self.lock:
            new_list = []
            removed = False
            for slave in self.slaves:
                if slave.ip == ip:
                    removed = True
                    if slave.status == "idle":
                        # remove it
                        continue
                    else:
                        if slave.remove_after_finish:
                            raise Exception("This slave is already marked for removal. Use --kill to remove it immediate or wait for the job to finish.")
                        if options["wait"]:
                            slave.remove_after_finish = True
                        elif options["kill"]:
                            pid = slave.running_job.pid
                            slave._shutdown_event.set()
                            slave.process_waiter.join()
                            os.kill(pid)
                            continue
                        else:
                            raise Exception("This slave is currently busy. Use --wait or --kill to remove it.")
                new_list.append(slave)
            self.slaves = new_list
            self._save()
        if removed:
            return {"msg": "ok"}
        else:
            return {"msg": f"Cannot find the specific slave with {ip}"}

    def load_status(self, file):
        with open(file, "r", encoding="utf-8") as f:
            j = json.load(f)
        self.job_waitlist = deque(j["job_waitlist"])
        self.slaves = []
        for slave in j["slaves"]:
            s = Slave(slave["ip"], slave["envs"], self)
            s.status = slave["status"]
            if "remove_after_finish" in slave:
                s.remove_after_finish = slave["remove_after_finish"]
            job_info = slave["running_job"]
            if job_info is not None:
                s.running_job = JobInfo(job_info["id"], job_info["envs"], job_info["script"], job_info["pid"], job_info["log_file"])
                s.monitor()
            self.slaves.append(s)
        self._save()
        return {"msg": "ok"}

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
        self.should_stop = True
        for slave in self.slaves:
            slave.shutdown()

    def schedule(self):
        """Schedule waiting jobs to idle slaves.
        Be careful this method will be called from different threads.
        """
        slave = None
        with self.lock:
            while len(self.job_waitlist) > 0:
                hasIdleSlave = False
                for slave in self.slaves:
                    if slave.status == "idle":
                        hasIdleSlave = True
                        job = self.job_waitlist.popleft()
                        slave.run(*job)
                        break
                if not hasIdleSlave:
                    break
        return slave

    def toJSON(self):
        return {
            "job_waitlist": list(self.job_waitlist),
            "slaves": [{
                "ip": slave.ip,
                "envs": slave.envs,
                "status": slave.status,
                "running_job": slave.running_job,
                **({"remove_after_finish": slave.remove_after_finish} if slave.remove_after_finish else {})
            } for slave in self.slaves]
        }


def check_pid(pid):
    """ Check For the existence of a unix pid. """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


class JobInfo:
    def __init__(self, id: str, script: str, envs: Dict[str, str], pid: int, log_file: str) -> None:
        self.id = id
        self.script = script
        self.envs = envs
        self.pid = pid
        self.log_file = log_file

    def is_alive(self):
        return check_pid(self.pid)

    def __str__(self) -> str:
        alive_status = "Alive" if self.is_alive() else "Dead"

        return f"""Job {self.id}: {alive_status}
    Script: {self.script}
    Last 10 lines of {self.log_file}:
    {subprocess.run(["tail", "-n", "10", self.log_file]).stdout.decode("utf-8")}
"""

def parse_key_values(envs):
    envs_dict = {}
    for env_str in envs:
        try:
            k, v = env_str.split("=")
            if k in ["JOB_ID", "SLAVE_IP"]:
                raise ValueError(f"Environment name {k} is reserved.")
            if k in envs_dict:
                logging.warn(f"Environment name {k} is duplicated. The new value {v} will overwrite its previous value {envs_dict[k]}.")
            envs_dict[k] = v
        except:
            raise ValueError(f"Failed to parse environment key-value pair: {env_str}. Every environment variable must be set as <NAME>=<VALUE>.")
    return envs_dict

class Slave:
    def __init__(self, ip, envs, ctx: ServerContext) -> None:
        self.ip = ip
        self.envs = parse_key_values(envs)
        self.status = "idle"
        self.running_job = None  # type: JobInfo
        self.ctx = ctx
        self.process_waiter = None
        self.remove_after_finish = False
        self._shutdown_event = threading.Event()

    def run(self, job_script: List[str], envs: List[str]):
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
        subprocess.run(["mkdir", "-p", self.ctx.logs_dir])
        log_file = os.path.join(self.ctx.logs_dir, f"job_{job_id}.txt")
        log_file = log_file.replace(" ", "\\ ")
        job_envs = parse_key_values(envs)
        envs_dict = {
            "JOB_ID": job_id,
            "SLAVE_IP": self.ip,
            **self.envs,
            **job_envs
        }
        proc = subprocess.Popen(" ".join(job_script) + f" >{log_file} 2>&1", env=envs_dict, shell=True, start_new_session=True)
        self.running_job = JobInfo(job_id, job_envs, job_script, proc.pid, log_file)
        self.monitor(proc)
        self.ctx._save()

    def monitor(self, proc=None):
        """Start a monitoring thread if there is a running job.
        The monitering thread will reset the slave's states Once the job is finished.
        """
        if self.running_job is not None and self.process_waiter is None:
            pid = self.running_job.pid
            self.process_waiter = threading.Thread(
                name=f"Job waiter {self.ip} {self.running_job.id}",
                target=_process_waiter, args=[pid, self, proc])
            self.process_waiter.start()

    def shutdown(self):
        if self.process_waiter is not None:
            self._shutdown_event.set()
            self.process_waiter.join()

    def __getstate__(self):
        return (self.ip, self.status, self.running_job, self.remove_after_finish)

    def __setstate__(self, state):
        """Make sure to call Slave.associate afterwards"""
        if len(state) == 3:
            self.ip, self.status, self.running_job = state
            self.remove_after_finish = False
        elif len(state) == 4:
            self.ip, self.status, self.running_job, self.remove_after_finish = state
        self.ctx = None

    def associate(self, ctx: ServerContext):
        """Associate this slave to a server context object. Only used after loading the server context object from file.

        Args:
            ctx (ServerContext): The Server context object.
        """
        if self.ctx is not None:
            logging.error("A slave can only be associated once.")
            return
        self.process_waiter = None
        self.ctx = ctx
        self._shutdown_event = threading.Event()
        self.monitor()


def _process_waiter(pid, slave: Slave, proc: subprocess.Popen):
    while check_pid(pid):
        if proc is not None and proc.poll() != None:
            break
        if slave._shutdown_event.wait(1):
            # slave is shutting down, keep its inner states intact for serialization later
            return

    logging.info(
        f"Finished job {slave.running_job.id} on {slave.ip} > {slave.running_job.log_file}")
    slave.process_waiter = None
    slave.running_job = None
    if slave.remove_after_finish:
        slave.ctx.slaves.remove(slave)
        slave.status = "removed"
    else:
        slave.status = "idle"
    slave.ctx._save()
    slave.ctx.schedule()


def start(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(
        description="Start the scheduler service.")
    parser.add_argument("--log_dir", type=str, default="logs")
    args = parser.parse_args(rest_args)
    data_dir = base_args.server_data_dir
    logs_dir = args.log_dir
    subprocess.run(["mkdir", "-p", data_dir])
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    pid_file_path = os.path.join(data_dir, "service_pid")

    def ensure_fifo():
        if not os.path.exists(commands_fifo_path):
            subprocess.run(["mkfifo", commands_fifo_path])

    if os.path.exists(commands_fifo_path):
        try:
            with open(pid_file_path, "r") as f:
                last_pid = int(f.read())
            if check_pid(last_pid):
                logging.error(
                    "`commands_fifo` already exists in this folder. Try shutting down the already running server with `python3 cli.py stop`")
                sys.exit(-1)
            subprocess.run(["rm", "-f", commands_fifo_path])
        except:
            pass

    with open(pid_file_path, "w") as f:
        f.write(str(os.getpid()))
    logging.info(f"Handling existing tasks")
    logging.info(f"Server started")

    ctx = ServerContext(data_dir, logs_dir)

    ensure_fifo()
    threading.Thread(name="print status", target=status,
                     args=[base_args, [], []]).start()

    ctx.schedule()
    while not ctx.should_stop:
        ensure_fifo()
        with open(commands_fifo_path, "r", encoding="utf-8") as f:
            try:
                cmd = json.load(f)
            except:
                logging.error("Error parsing json from stream")
                continue
            fifo_name = cmd['pipe'] if 'pipe' in cmd else None
            try:
                if cmd['type'] == 'shutdown':
                    logging.info(f"Shutting down")
                    ctx.shutdown()
                    resp = {"msg": "Stopped"}
                elif cmd['type'] == 'add_job':
                    resp = ctx.add_job(cmd['args'], cmd['envs'])
                elif cmd['type'] == 'add_slave':
                    resp = ctx.add_slave(cmd['ip'], cmd['envs'])
                elif cmd['type'] == 'status':
                    resp = ctx.toJSON()
                elif cmd['type'] == 'remove_job':
                    resp = ctx.remove_job(cmd['args'], cmd['envs'])
                elif cmd['type'] == 'remove_slave':
                    resp = ctx.remove_slave(cmd['ip'], cmd['options'])
                elif cmd['type'] == 'load_status':
                    resp = ctx.load_status(cmd['file'])
                else:
                    resp = {"err": f"Unknown command type {cmd['type']}"}
            except Exception as err:
                traceback.print_exc()
                resp = {"err": str(err)}
            if fifo_name is not None:
                with open(fifo_name, "w", encoding="utf-8") as f:
                    f.write(json.dumps(resp, default=dumper) + "\n")


class Fifo:
    def __init__(self, data_dir) -> None:
        id = str(int(time.time() * 1000))
        self.fifo_name = os.path.join(data_dir, f"tmp_{id}")
        if os.path.exists(self.fifo_name):
            subprocess.run(["rm", "-f", self.fifo_name])
        subprocess.run(["mkfifo", self.fifo_name])

    def done(self):
        with open(self.fifo_name, "r", encoding="utf-8") as f:
            resp = json.loads(f.readline())
        subprocess.run(["rm", "-f", self.fifo_name])
        if "err" in resp:
            logging.error(resp["err"])
            return None
        if "msg" in resp:
            logging.info(resp["msg"])
        return resp


def communicate(data_dir, commands_fifo_path, obj):
    fifo = Fifo(data_dir)
    obj["pipe"] = fifo.fifo_name
    with open(commands_fifo_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(obj, default=dumper) + "\n")
    return fifo.done()


def stop(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    communicate(data_dir, commands_fifo_path, {
        "type": "shutdown"
    })


def add_job(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(description="Add a job")
    parser.add_argument("--env,-e", type=str, nargs='*',
                        help="Set additional environment variable to the job script.")
    args, rest_args = parser.parse_known_args(rest_args)
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    if len(rest_args) == 0:
        logging.error("Usage: add_job <template.sh> [arg1] [arg2] ...")
        sys.exit(-1)

    if not os.path.isfile(rest_args[0]):
        logging.error(f"{rest_args[0]} does not exist")
        sys.exit(-1)

    if not rest_args[0].startswith("./"):
        rest_args[0] = "./" + rest_args[0]

    communicate(data_dir, commands_fifo_path, {
        "type": "add_job",
        "args": rest_args + user_args,
        "envs": args.env if 'env' in args else []
    })


def remove_job(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(description="Remove a job")
    parser.add_argument("--env,-e", type=str, nargs='*',
                        help="Set additional environment variable to the job script.")
    args, rest_args = parser.parse_known_args(rest_args)
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    if len(rest_args) == 0:
        logging.error("Usage: remove_job <template.sh> [arg1] [arg2] ...")
        sys.exit(-1)

    if not rest_args[0].startswith("./"):
        rest_args[0] = "./" + rest_args[0]

    communicate(data_dir, commands_fifo_path, {
        "type": "remove_job",
        "args": rest_args + user_args,
        "envs": args.env if 'env' in args else []
    })


def add_slave(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(description="Add a slave.")
    parser.add_argument("ip", type=str, nargs='+',
                        help="IP addresses of the slaves")
    parser.add_argument("--skip_ssh_auth_check", action="store_true",
                        help="Whether to skip the check of slave supporting ssh no-password authentication.")
    parser.add_argument("--env,-e", type=str, nargs='*',
                        help="Set additional environment variable to the job script when executing on this slave.")
    args = parser.parse_args(rest_args)
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    for ip in args.ip:
        if not args.skip_ssh_auth_check:
            p = subprocess.run(
                ["ssh", "-o", "PasswordAuthentication=no", ip, "/bin/true"])
            if p.returncode != 0:
                logging.error(
                    f"Password login is still required for ssh {ip}. Please ensure no password is needed to ssh into {ip}.")
                sys.exit(-1)

        communicate(data_dir, commands_fifo_path, {
            "type": "add_slave",
            "ip": ip,
            "envs": args.env if 'env' in args else []
        })


def remove_slave(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(description="Remove a slave.")
    parser.add_argument("ip", type=str, nargs='+',
                        help="IP addresses of the slaves")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--kill", action="store_true")
    args = parser.parse_args(rest_args)
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    if args.wait and args.kill:
        logging.error("Only one of --wait and --kill should be used at a time.")
        sys.exit(-1)

    options = {
        "wait": args.wait,
        "kill": args.kill
    }

    for ip in args.ip:
        communicate(data_dir, commands_fifo_path, {
            "type": "remove_slave",
            "ip": ip,
            "options": options
        })


def status(args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    data_dir = args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    resp = communicate(data_dir, commands_fifo_path, {
        "type": "status",
    })
    print(json.dumps(resp, ensure_ascii=False, indent=2))


def load_status(base_args: argparse.Namespace, rest_args: List[str], user_args: List[str]):
    parser = ArgumentParser(description="Load status from a JSON file.")
    parser.add_argument("file", type=str)
    args = parser.parse_args(rest_args)
    data_dir = base_args.server_data_dir
    commands_fifo_path = os.path.join(data_dir, "commands_fifo")
    if not os.path.exists(commands_fifo_path):
        logging.error(
            "{commands_fifo_path} does not exists. Try starting a server with `python3 cli.py start`")
        sys.exit(-1)

    communicate(data_dir, commands_fifo_path, {
        "type": "load_status",
        "file": args.file
    })


if __name__ == "__main__":
    argv = sys.argv[1:]
    try:
        force_stop_index = argv.index("--")
        argv = argv[: force_stop_index]
        user_args = argv[force_stop_index + 1:]
    except ValueError:
        force_stop_index = -1
        user_args = []
    parser = ArgumentParser(
        description="Scheduler service command line interface.", add_help=False)
    parser.add_argument("action", type=str, default="status",
                        help="One of start/stop/status/add_job/add_slave/remove_slave/load_status")
    parser.add_argument("--server_data_dir", type=str, default=".data")

    main_parser = ArgumentParser(parents=[parser])

    args, rest_args = main_parser.parse_known_args(sys.argv[1:])
    action = args.action
    logging.basicConfig(level=logging.INFO)
    if action == "status":
        status(args, rest_args, user_args)
    elif action == "start":
        start(args, rest_args, user_args)
    elif action == "stop":
        stop(args, rest_args, user_args)
    elif action == "add_job":
        add_job(args, rest_args, user_args)
    elif action == "remove_job":
        remove_job(args, rest_args, user_args)
    elif action == "add_slave":
        add_slave(args, rest_args, user_args)
    elif action == "remove_slave":
        remove_slave(args, rest_args, user_args)
    elif action == "load_status":
        load_status(args, rest_args, user_args)
    else:
        parser.print_help()
