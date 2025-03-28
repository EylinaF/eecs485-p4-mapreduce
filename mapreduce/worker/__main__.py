"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import mapreduce.utils
import threading
import socket

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        signals = {"shutdown": False}
        self.threads = []
        listener_thread = threading.Thread(target=self.start_tcp_listener, args=(signals,))
        listener_thread.start()
        self.threads.append(listener_thread)
        self.register()
        heartbeat_thread = threading.Thread(target=self.send_heartbeats, args=(signals,))
        self.threads.append(heartbeat_thread)
        heartbeat_thread.start()
        for thread in self.threads:
            thread.join()
        LOGGER.info("Worker shut down")

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
                    if message_dict.get("message_type") == "shutdown":
                        LOGGER.info("Worker received shutdown")
                        signals["shutdown"] = True
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    LOGGER.warning("Invalid TCP message: %s", e)
                    continue

    def register(self):
        """Send register message and wait for register_ack."""
        msg = {
            "message_type": "register",
            "worker_host": self.host,
            "worker_port": self.port,
        }
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.manager_host, self.manager_port))
                sock.sendall(json.dumps(msg).encode("utf-8"))
                sock.settimeout(2)
                response = sock.recv(4096).decode("utf-8")
                ack = json.loads(response)
                LOGGER.debug("Got register_ack %s", ack)
        except Exception as e:
            LOGGER.error("Failed to register with Manager: %s", e)


    def send_heartbeats(self, signals):
            """Periodically send heartbeat messages to the Manager via UDP."""
            msg = {
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port,
            }
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                while not signals["shutdown"]:
                    try:
                        sock.sendto(json.dumps(msg).encode("utf-8"), (self.manager_host, self.manager_port))
                        LOGGER.debug("Sent heartbeat to Manager %s:%s", self.manager_host, self.manager_port)
                        time.sleep(1)
                    except Exception as e:
                        LOGGER.warning("Failed to send heartbeat: %s", e)

    
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
