"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import socket
import time
import queue
import click
import mapreduce.utils
import threading
from .helper import JobManager

# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )


        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        prefix = f"mapreduce-shared-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            self.shared_dir = tmpdir
            self.host = host
            self.port = port
            self.job_manager = JobManager(self.shared_dir)
            self.registered_workers = []  # Preserve registration order
            self.worker_busy = {}  # { (host, port): True/False }
            self.pending_tasks = queue.Queue()
            self.tasks_in_progress = set()
            signals = {"shutdown": False}
            self.threads = []
            udp_thread = threading.Thread(target=self.udp_listening, args=(signals,))
            self.threads.append(udp_thread)
            udp_thread.start()
            LOGGER.info("started udp thread listener")
            tcp_thread = threading.Thread(target=self.start_tcp_listener, args=(signals,))
            self.threads.append(tcp_thread)
            tcp_thread.start()
            for thread in self.threads:
                thread.join()
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)
        


    def udp_listening(self, signals):
        """Listen for UDP heartbeat messages from workers."""

        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.settimeout(1)

            # Listen for heartbeat messages
            while not signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                try:
                    message_str = message_bytes.decode("utf-8")
                    message_dict = json.loads(message_str)
                    LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    LOGGER.warning("Invalid UDP message: %s", e)
                    continue
    
    def start_tcp_listener(self, signals):
        """Start the TCP listener for incoming messages from Workers."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)
            LOGGER.debug("TCP bind %s:%s", self.host, self.port)

            while not signals["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                LOGGER.debug("Connection from %s", address[0])
                clientsocket.settimeout(1)

                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                    message_bytes = b"".join(message_chunks)
                    try:
                        message_str = message_bytes.decode("utf-8")
                        message_dict = json.loads(message_str)
                        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
                        if message_dict["message_type"] == "register":
                                worker = (message_dict["worker_host"], message_dict["worker_port"])
                                if worker not in self.registered_workers:
                                        self.registered_workers.append(worker)
                                        self.worker_busy[worker] = False
                                ack = {"message_type": "register_ack"}
                                LOGGER.info("Sending register_ack to %s", worker)
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                    sock.connect(worker)
                                    sock.sendall(json.dumps(ack).encode("utf-8"))
                                    LOGGER.info("Sent register_ack to %s", worker)
                                LOGGER.info("Registered Worker %s", worker)
                        
                        
                        elif message_dict["message_type"] == "new_manager_job":
                                job_id = self.job_manager.new_job_request(message_dict)
                                LOGGER.info(f"Received new job request, assigned Job ID: {job_id}")
                                self.job_manager.run_job()  # Generates tasks
                                # Queue all tasks for the current job
                                for task in self.job_manager.current_job_tasks:
                                    self.pending_tasks.put(task)
                                self._assign_tasks()

                        elif message_dict["message_type"] == "task_complete":
                                worker_host = message_dict["worker_host"]
                                worker_port = message_dict["worker_port"]
                                task_id = message_dict["task_id"]
                                worker = (worker_host, worker_port)
                                # Mark worker as available
                                self.worker_busy[worker] = False
                                # Remove task from in-progress set
                                self.tasks_in_progress.discard(task_id)
                                # Assign next tasks
                                self._assign_tasks()
                                # Check if job is completed
                                if self.pending_tasks.empty() and not self.tasks_in_progress:
                                    LOGGER.info("Current job completed")
                                    self.job_manager.current_job = None

                        elif message_dict["message_type"] == "shutdown":
                            LOGGER.info("Received shutdown message")
                            shutdown_msg = json.dumps({"message_type": "shutdown"}).encode("utf-8")
                            for host, port in self.registered_workers:
                                try:
                                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                                        sock.connect((host, port))
                                        sock.sendall(shutdown_msg)
                                        LOGGER.info("Sent shutdown to Worker %s:%s", host, port)
                                except Exception as e:
                                    LOGGER.warning("Failed to send shutdown to %s:%s: %s", host, port, e)
                            signals["shutdown"] = True

                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        LOGGER.warning("Invalid TCP message: %s", e)
                        continue

    def _send_task_to_worker(self, task, worker):
        message = {
            "message_type": "new_map_task",
            "task_id": task["task_id"],
            "input_paths": task["input_paths"],
            "executable": task["executable"],
            "output_directory": task["output_directory"],
            "num_partitions": task["num_partitions"],
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(worker)
                sock.sendall(json.dumps(message).encode("utf-8"))
                self.worker_busy[worker] = True
                self.tasks_in_progress.add(task["task_id"])
                LOGGER.info(f"Sent task {task['task_id']} to {worker}")
        except Exception as e:
            LOGGER.error(f"Failed to send task to {worker}: {e}")
            self.pending_tasks.put(task)  # Requeue the task

    def _assign_tasks(self):
        while not self.pending_tasks.empty():
            task = self.pending_tasks.queue[0]  # Peek the next task
            for worker in self.registered_workers:
                if not self.worker_busy.get(worker, False):
                    self.pending_tasks.get()  # Remove the task from the queue
                    self._send_task_to_worker(task, worker)
                    break
            else:
                break


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    print("started to run main")
    tempfile.tempdir = shared_dir
    if logfile: 
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    print("ready to create manager")
    Manager(host, port)


if __name__ == "__main__":
    main()
