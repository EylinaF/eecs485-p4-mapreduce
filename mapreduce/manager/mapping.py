import os
import tempfile
import logging
import json
import socket
import time
import click
import mapreduce.utils
import threading

def mapping(self, signals):
    while not signals["shutdown"]:
        