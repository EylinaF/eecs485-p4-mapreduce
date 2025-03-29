import subprocess
import logging
import heapq
from pathlib import Path

LOGGER = logging.getLogger(__name__)

class WorkerTasks:
    def __init__(self, worker_host, worker_port, manager_host, manager_port):
        self.host = worker_host
        self.port = worker_port
        self.manager_host = manager_host
        self.manager_port = manager_port

    def send_task_finished(self, task_id, task_type="reduce"):
        """Notify Manager that a task has completed."""
        msg = {
            "message_type": "task_complete",
            "task_id": task_id,
            "worker_host": self.host,
            "worker_port": self.port,
            "task_type": task_type
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode("utf-8"))
                LOGGER.info("Sent %s task %d completion", task_type, task_id)
        except Exception as e:
            LOGGER.error("Failed to send task completion: %s", e)

    def handle_reduce_task(self, task):
        """Execute a reduce task with proper merging and sorting."""
        task_id = task["task_id"]
        input_paths = task["input_paths"]
        executable = task["executable"]
        output_directory = Path(task["output_directory"])

        LOGGER.info("Starting reduce task %d with %d input files", 
                   task_id, len(input_paths))

        try:
            # Create output file
            output_file = output_directory / f"part-{task_id:05d}"
            
            # Open all input files
            files = [open(path, "r") for path in input_paths]
            
            # Use merge-sort approach
            with open(output_file, "w") as outfile:
                merged = heapq.merge(*files)
                
                # Process through the reducer
                with subprocess.Popen(
                    [executable],
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                    text=True,
                ) as proc:
                    for line in merged:
                        proc.stdin.write(line)
                    proc.stdin.close()
                    proc.wait()

        except Exception as e:
            LOGGER.error("Reduce task %d failed: %s", task_id, e)
            raise
        finally:
            # Close all files
            for f in files:
                if hasattr(f, 'close'):
                    f.close()

        self.send_task_finished(task_id, "reduce")