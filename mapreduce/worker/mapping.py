import subprocess
import tempfile
import hashlib
import shutil
import logging
import socket
import json
from pathlib import Path

LOGGER = logging.getLogger(__name__)

def send_task_finished(self, task_id):
    msg = {
        "message_type": "finished",
        "task_id": task_id,
        "worker_host": self.host,
        "worker_port": self.port,
    }

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode("utf-8"))
                LOGGER.debug("Sent finished message for task %d", task_id)
    except Exception as e:
            LOGGER.error("Failed to send finished message: %s", e)

def handle_map_task(self, task):
    task_id = task["task_id"]
    input_paths = task["input_paths"]
    executable = task["executable"]
    output_directory = Path(task["output_directory"])
    num_partitions = task["num_partitions"]

    LOGGER.info("Started map task %d", task_id)

    with tempfile.TemporaryDirectory(prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
        tmpdir_path = Path(tmpdir)

        partition_files = {
            i: open(tmpdir_path / f"maptask{task_id:05d}-part{i:05d}", "w")
            for i in range(num_partitions)
        }

        try:
            for input_path in input_paths:
                with open(input_path, "r") as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as proc:
                        for line in proc.stdout:
                            key = line.split("\t", 1)[0]
                            partition = int(
                                hashlib.md5(key.encode("utf-8")).hexdigest(), 16
                            ) % num_partitions
                            partition_files[partition].write(line)

            for f in partition_files.values():
                f.close()

            for i in range(num_partitions):
                filename = tmpdir_path / f"maptask{task_id:05d}-part{i:05d}"
                subprocess.run(["sort", "-o", filename, filename], check=True)

            output_directory.mkdir(parents=True, exist_ok=True)
            for i in range(num_partitions):
                src = tmpdir_path / f"maptask{task_id:05d}-part{i:05d}"
                dst = output_directory / src.name
                shutil.move(str(src), str(dst))

        finally:
            for f in partition_files.values():
                if not f.closed:
                    f.close()

    self.send_task_finished(task_id)

