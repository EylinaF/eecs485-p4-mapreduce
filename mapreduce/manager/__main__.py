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
            self.current_phase = None
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
                            if not data:
                                break
                            message_chunks.append(data)
                            # Attempt to parse the message
                            message_bytes = b"".join(message_chunks)
                            try:
                                message_str = message_bytes.decode("utf-8")
                                message_dict = json.loads(message_str)
                                # Process the message
                                self._process_message(message_dict, signals)
                                message_chunks = []  # Reset after successful parse
                            except json.JSONDecodeError:
                                # Incomplete message, continue reading
                                continue
                            except UnicodeDecodeError as e:
                                LOGGER.warning("Decode error: %s", e)
                                break
                        except socket.timeout:
                            break


    def _process_message(self, message_dict, signals):
        """Handle a parsed JSON message."""
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
                LOGGER.info(f"Received new job, assigned Job ID: {job_id}")
                self.job_manager.run_job()  # Generates map tasks
                for task in self.job_manager.current_job_tasks:
                    self.pending_tasks.put(task)
                self.current_phase = "map"  # Start with map phase
                self._assign_tasks()

        elif message_dict["message_type"] == "finished":
                worker_host = message_dict["worker_host"]
                worker_port = message_dict["worker_port"]
                task_id = message_dict["task_id"]
                worker = (worker_host, worker_port)
                self.worker_busy[worker] = False
                self.tasks_in_progress.discard(task_id)
                                
                if self.pending_tasks.empty() and not self.tasks_in_progress:
                    if self.current_phase == "map":
                        # Generate reduce tasks
                        job_id = self.job_manager.current_job["job_id"]
                        intermediate_dir = os.path.join(
                                self.shared_dir, f"job-{job_id:05d}"
                        )
                        reduce_tasks = self.job_manager.create_reduce_tasks(intermediate_dir)
                        for task in reduce_tasks:
                            self.pending_tasks.put(task)
                        self.current_phase = "reduce"
                    elif self.current_phase == "reduce":
                        # Clean up intermediate directory
                        self._handle_job_completion(signals)
                self._assign_tasks()
                    
                                        

        elif message_dict["message_type"] == "shutdown":
                LOGGER.info("Received shutdown message")
    
                # Clean up current job
                if self.job_manager.current_job:
                    job_id = self.job_manager.current_job["job_id"]
                    self.job_manager.cleanup_job(job_id)
    
                # Clean up all queued jobs
                while not self.job_manager.job_queue.empty():
                    job = self.job_manager.job_queue.get()
                    self.job_manager.cleanup_job(job["job_id"])
    
                # Send shutdown to workers
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
                

    

    def send_task_to_worker(self, task, worker):
        """Send either new_map_task or new_reduce_task."""
        # Determine message type based on task keys
        if "num_partitions" in task:
            message_type = "new_map_task"
        else:
            message_type = "new_reduce_task"

        message = {
            "message_type": message_type,
            "task_id": task["task_id"],
            "input_paths": task["input_paths"],
            "executable": task["executable"],
            "output_directory": task["output_directory"],
        }
        if message_type == "new_map_task":
            message["num_partitions"] = task["num_partitions"]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(worker)
                sock.sendall(json.dumps(message).encode("utf-8"))
                self.worker_busy[worker] = True
                self.tasks_in_progress.add(task["task_id"])
                LOGGER.info(f"Sent {message_type} {task['task_id']} to {worker}")
        except Exception as e:
            LOGGER.error(f"Failed to send task to {worker}: {e}")
            self.pending_tasks.put(task)  # Requeue


    def _assign_tasks(self):
        """Assign tasks (map or reduce) to available workers."""
        while not self.pending_tasks.empty():
            task = self.pending_tasks.queue[0]  # Peek next task
            for worker in self.registered_workers:
                if not self.worker_busy.get(worker, False):
                    self.pending_tasks.get()
                    self.send_task_to_worker(task, worker)
                    break
            else:
                break 

    def _handle_job_completion(self, signals):
        """Handle job completion and shutdown if no more jobs."""
        if self.job_manager.current_job:
            job_id = self.job_manager.current_job["job_id"]
            self.job_manager.cleanup_job(job_id)
            self.job_manager.current_job = None
            self.current_phase = None

        if not self.job_manager.job_queue.empty():
            # Start next job
            self.job_manager.run_job()
            for task in self.job_manager.current_job_tasks:
                self.pending_tasks.put(task)
            self.current_phase = "map"
            self._assign_tasks()

        else:
            # Send shutdown to workers
            LOGGER.info("All jobs completed. Shutting down workers.")
            shutdown_msg = json.dumps({"message_type": "shutdown"}).encode("utf-8")
            for worker in self.registered_workers:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                        sock.connect(worker)
                        sock.sendall(shutdown_msg)
                        LOGGER.info(f"Sent shutdown to {worker}")
                except Exception as e:
                    LOGGER.warning(f"Failed to send shutdown to {worker}: {e}")
            signals["shutdown"] = True

    def _start_reduce_phase(self):
        """Transition from map to reduce phase."""
        job_id = self.current_job["job_id"]
        intermediate_dir = os.path.join(self.shared_dir, f"job-{job_id:05d}")
        reduce_tasks = self._create_reduce_tasks(intermediate_dir)
    
        for task in reduce_tasks:
            self.pending_tasks.put(task)
        self.current_phase = "reduce"
        self._assign_tasks()



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
