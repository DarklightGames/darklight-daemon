import time
import requests
import json
import os
import argparse
import glob
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv

load_dotenv()

host = os.getenv('API_HOST')
secret = os.getenv('API_SECRET')

class LogManifest(object):

    def __init__(self, file: str):
        self.file = file
        self.accepted = set()
        self.rejected = set()
        try:
            data = json.load(open(file, 'r'))
            self.accepted.update(data['accepted'])
            self.rejected.update(data['rejected'])
        except FileNotFoundError:
            pass

    def is_handled(self, file) -> bool:
        return file in self.accepted or file in self.rejected

    def is_rejected(self, file) -> bool:
        return file in self.rejected

    def add_accepted(self, file):
        self.accepted.add(file)
        self.save()

    def add_rejected(self, file):
        self.rejected.add(file)
        self.save()

    def save(self):
        data = dict()
        data['accepted'] = list(self.accepted)
        data['rejected'] = list(self.rejected)
        json.dump(data, open(self.file, 'w'), indent=2)

manifest = LogManifest('manifest.json')

def send_log(file):
    print('Sending {}...'.format(file))
    url = host + 'logs/'
    data = {
        'secret': secret
    }
    files = {
        'log': open(file, 'rb')
    }
    response = requests.post(url, data, files=files)
    if response.status_code in [201, 409]:
        # The server accepted it (201) or rejected it as a log that has already
        # been parsed (409). Let's throw it in the acepted pile.
        print('Log file processed ({})'.format(response.status_code))
        manifest.add_accepted(os.path.basename(file))
        pass
    elif response.status_code == 500:
        print('Log file rejected ({})'.format(response.status_code))
        # Internal server error while parsing the log, throw it in the rejected pile.
        # The server should probably be *storing* the logs before we get here, the
        # server could keep track of what the error is and correct it if need be.
        manifest.add_rejected(os.path.basename(file))
        pass
    else:
        # The server rejected it for an unknown reason. We will do nothing
        # and the daemon can attempt to send it up again in the future.
        print('Communication breakdown, forget it!')
        pass

class DaemonFileHandler(FileSystemEventHandler):
    def on_moved(self, event):
        # The game writes to a .log.tmp file then later renames (moves) it
    	# with the correct extension.
        if os.path.splitext(event.dest_path)[1] == '.log':
            print('A wild log file appeared!')
            send_log(event.dest_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', action='append')
    parser.add_argument('--skip_backlog', action='store_true')
    parser.add_argument('--retry_rejected', action='store_true', default=False)
    args = parser.parse_args()
    paths = args.path

    if paths is None or len(paths) == 0:
        parser.print_usage()
        raise Exception('No paths specified.')

    # For each path, go through all the *.log files (perhaps a CRC-based method
    # would be better instead of filename, since file names can overlap.
    # The other option would be to put the manifest in the path itself
    # then we can still do file based stuff.
    if not args.skip_backlog:
        print('Clearing backlog...')
        for path in paths:
            # For each path, check if there are unhandled log
            # files and then attempt to send them all to
            # the server.
            for file in glob.glob(os.path.join(path, '*.log')):
                if not manifest.is_handled(os.path.basename(file)):
                    send_log(file)

    if args.retry_rejected:
        print('Retrying {} rejected files...'.format(len(manifest.rejected)))
        for path in paths:
            for file in glob.glob(os.path.join(path, '*.log')):
                if manifest.is_rejected(os.path.basename(file)):
                    send_log(file)

    print('Backlog cleared.')

    event_handler = DaemonFileHandler()
    observers = []

    for path in paths:
        # Set up a new file observer in each specified path.
        observer = Observer()
        print('Listening at "{}"'.format(path))
        observer.schedule(event_handler, path=path, recursive=False)
        observer.start()
        observers.append(observer)

    # Sit and spin until we are interrupted.
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for observer in observers:
            observer.stop()
    for observer in observers:
        observer.join()
