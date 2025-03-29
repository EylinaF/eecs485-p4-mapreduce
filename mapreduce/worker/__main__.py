"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import threading
import socket
from pathlib import Path

from .mapping import WorkerTasks as MapWorker
from .reducing import WorkerTasks as ReduceWorker

LOGGER = logging.getLogger(__name__)

class Worker:
    def __init__(self, host, port, manager_host, manager_port):
        """Initialize worker with mapping and reducing capabilities."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        
        # Initialize task handlers
        self.map_worker = MapWorker(host, port, manager_host, manager_port)
        self.reduce_worker = ReduceWorker(host, port, manager_host, manager_port)
        
        signals = {"shutdown": False, "registered": False}
        self.threads = [
            threading.Thread(target=self.start_tcp_listener, args=(signals,)),
            threading.Thread(target=self.send_heartbeats, args=(signals,))
        ]
        
        self.register()
        
        for thread in self.threads:
            thread.start()
        
        for thread in self.threads:
            thread.join()
        
        LOGGER.info("Worker shut down")

    def start_tcp_listener(self, signals):
        """Handle incoming tasks from Manager with proper message validation."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            sock.settimeout(1)

            while not signals["shutdown"]:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue

                with clientsocket:
                    clientsocket.settimeout(1)
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                            if not data:  # Connection closed
                                break
                            message_chunks.append(data)
                        except socket.timeout:
                            break

                    message_bytes = b"".join(message_chunks)
                    if not message_bytes:
                        LOGGER.debug("Received empty message, skipping")
                        continue

                    try:
                        message_str = message_bytes.decode("utf-8")
                        message_dict = json.loads(message_str)
                        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
                    
                        # Handle message types
                        if message_dict.get("message_type") == "shutdown":
                            signals["shutdown"] = True
                            break  # Exit inner loop
                        elif message_dict.get("message_type") == "register_ack":
                            signals["registered"] = True
                        elif message_dict["message_type"] == "new_map_task":
                            threading.Thread(
                                target=self.map_worker.handle_map_task,
                                args=(message_dict,)
                            ).start()
                        elif message_dict["message_type"] == "new_reduce_task":
                            threading.Thread(
                                target=self.reduce_worker.handle_reduce_task,
                                args=(message_dict,)
                            ).start()

                    except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
                        LOGGER.warning("Invalid message: %s", e)

                # Exit outer loop immediately after shutdown
                if signals["shutdown"]:
                    break

    def register(self):
        """Register with Manager."""
        msg = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode())
        except Exception as e:
            LOGGER.error("Registration failed: %s", e)

    def send_heartbeats(self, signals):
        """Send periodic heartbeats to Manager."""
        msg = {
            "message_type": "heartbeat",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while not signals["shutdown"]:
                try:
                    sock.sendto(json.dumps(msg).encode(), 
                              (self.manager_host, self.manager_port))
                    time.sleep(1)
                except Exception as e:
                    LOGGER.warning("Heartbeat failed: %s", e)

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)

if __name__ == "__main__":
    main()