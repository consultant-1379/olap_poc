#!/usr/bin/python
from Queue import Queue
from datetime import datetime
import glob
from optparse import OptionParser
import os
import re
from subprocess import Popen, PIPE, STDOUT
from threading import Thread, Semaphore
import time
import sys
import traceback

DB_NAME = 'OLAP_POC'
DB_SERVER = '192.168.1.1'
DB_PORT = '5029'
DB_USER = 'dba'
DB_PASSWD = 'sql'
ROP_CSV_BASE = '/var/datagen'

IO = Semaphore(1)


def execute_process(command, verbose=None):
    if not verbose:
        verbose = []
    if 'cmd' in verbose:
        ioprint(' '.join(command))
    process = Popen(command, stdout=PIPE, stderr=STDOUT)
    stdout, stderr = process.communicate()
    if process.returncode:
        raise Exception(stdout.strip())
    else:
        return stdout.strip().split('\n')


def mysql(sql_command):
    m = ['/usr/bin/mysql-ib', '--host={0}'.format(DB_SERVER), '--port={0}'.format(DB_PORT),
         '--user={0}'.format(DB_USER), '--password={0}'.format(DB_PASSWD), '--skip-column-names',
         DB_NAME, '--execute=\'{0}\''.format(sql_command)]
    return execute_process(['ssh', 'root@{0}'.format(DB_SERVER), '{0}'.format(' '.join(m))])


def truncate(tables=None):
    if tables is None:
        tables = mysql('SHOW TABLES')
    for table in tables:
        ioprint('Deleting data from table {0}'.format(table))
        mysql('DELETE FROM {0}'.format(table))


def get_rop_info_files(rop_info_files=None):
    if '*' == rop_info_files:
        files = glob.glob(ROP_CSV_BASE + '/rop-*.txt')
        _map = {}
        for f in files:
            filename = os.path.basename(f)
            _match = re.match('rop-(.*).txt', filename)
            if _match:
                _map[_match.group(1)] = f
        skeys = sorted(_map.keys(), key=int)
        files = []
        for k in skeys:
            files.append(_map[k])
        return files
    else:
        return rop_info_files.split(',')


def dlp(dlp_info, dlp_instance, table, csv, packrow_size, verbose=None):
    if verbose is None:
        verbose = []
    dataprocessor_input_buffer_mb = dlp_info['buffer']
    dataprocessor_heap_mb = dlp_info['heap']
    compression_workthreads = dlp_info['cthreads']
    dlp_license_file = dlp_info['license']
    _id = '{0}@{1}'.format(dlp_info['host'], dlp_instance)
    ioprint('{0} Loading {1} with config: InputBufferMB:{2} HeapMB:{3} CompressThreads:{4} PackrowSize:{5}'.format(
        _id, table, dataprocessor_input_buffer_mb, dataprocessor_heap_mb, compression_workthreads, packrow_size))
    dlp_verbose = ''
    if 'dlp' in verbose:
        dlp_verbose = '--verbose'
    packrow = ''
    if packrow_size is not None:
        packrow = '--packrow-size={0}'.format(packrow_size)
    dlp_command = [
        'echo start:$(($(date +%s%N)/1000000));',
        dlp_instance, '--input-type=file', '--input-path={0}'.format(csv), '--host={0}'.format(DB_SERVER),
        '--port={0}'.format(DB_PORT), '-L', DB_USER, '-p', DB_PASSWD,
        '--LicenseFile={0}'.format(dlp_license_file), '--data-format=txt_variable',
        '--fields-terminated-by', '\',\'', '--fields-enclosed-by', '\\\'', '--lines-terminated-by', '\'\n\'',
        '--buffer-size={0}'.format(dataprocessor_input_buffer_mb),
        '--heap-size={0}'.format(dataprocessor_heap_mb),
        '--workers={0}'.format(compression_workthreads),
        packrow,
        dlp_verbose,
        # dlp_logfile,
        '--database={0}'.format(DB_NAME), '--table={0}'.format(table), '--execute-load;',
        'echo exit:$?;',
        'echo end:$(($(date +%s%N)/1000000))']
    return execute_process(
        ['ssh', '{0}@{1}'.format(dlp_info['user'], dlp_info['host']), '{0}'.format(' '.join(dlp_command))], verbose)


def ioprint(msg):
    IO.acquire()
    try:
        sys.stdout.write(msg)
        sys.stdout.write('\n')
        sys.stdout.flush()
    finally:
        IO.release()


def ioerror(msg):
    IO.acquire()
    try:
        sys.stderr.write(msg)
        sys.stderr.write('\n')
    finally:
        IO.release()


class LoaderThread():
    def __init__(self, dlp_pool, table, csvfile, packrow_size, file_line_count, verbose=None):
        if verbose is None:
            self.verbose = []
        else:
            self.verbose = verbose
        self.packrow_size = packrow_size
        self.dlp_pool = dlp_pool
        self.table = table
        self.csvfile = csvfile
        self.exception = None
        self.load_time = -1
        self.load_start_time = -1
        self.load_end_time = -1
        self.dlp_load_info = None
        self.file_line_count = file_line_count

    def load(self):
        dlp_info, instance = self.dlp_pool.get()
        try:
            results = dlp(dlp_info, instance, self.table, self.csvfile, self.packrow_size, self.verbose)
            if len(self.verbose) > 0:
                IO.acquire()
                try:
                    for line in results:
                        print('{0}@{1} {2}'.format(dlp_info['host'], instance, line))
                finally:
                    IO.release()
            _start_time = -1
            _end_time = -1
            _dlp_exit = -1
            for line in results:
                if line.startswith('start:'):
                    _start_time = long(line.split(':', 1)[1])
                elif line.startswith('end:'):
                    _end_time = long(line.split(':', 1)[1])
                elif line.startswith('exit:'):
                    _dlp_exit = int(line.split(':', 1)[1])
            self.load_time = _end_time - _start_time
            self.load_start_time = datetime.fromtimestamp(_start_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            self.load_end_time = datetime.fromtimestamp(_end_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            self.dlp_load_info = '{0}@{1}'.format(dlp_info['host'], instance)
            if _dlp_exit == 0:
                ioprint('\tLoaded {0}'.format(self.table))
            else:
                _msg = '\tDLP {0}@{1} exited non-zero code: {2}'.format(dlp_info['host'], instance, ' '.join(results))
                ioprint(_msg)
                raise IOError(_dlp_exit, _msg)
        except BaseException as e:
            traceback.print_exc()
            self.exception = e
        finally:
            self.dlp_pool.put([dlp_info, instance])

    def start(self):
        self._thread = Thread(target=self.load)
        self._thread.start()

    def join(self):
        if self.exception:
            raise self.exception
        self._thread.join()
        if self.exception:
            raise self.exception

    def error_cought(self):
        return self.exception

    def get_load_time(self):
        return self.load_time

    def get_load_file(self):
        return self.csvfile

    def get_load_start_time(self):
        return self.load_start_time

    def get_load_end_time(self):
        return self.load_end_time

    def get_load_table(self):
        return self.table

    def get_dlp_load_info(self):
        return self.dlp_load_info

    def get_row_count(self):
        return self.file_line_count


def load_rop(rop_info_file, dlp_pool, verbose=None):
    if not verbose:
        verbose = []
    ioprint('Loading data based on {0}'.format(rop_info_file))
    rop_load_start_time = time.time()
    with open(rop_info_file) as f:
        load_threads = []
        for line in f:
            line = line.strip()
            if len(line) == 0 or line.startswith('#'):
                continue
            parts = line.split(os.pathsep)
            _table = parts[0]
            _csvfile = parts[1]
            _rows = parts[2]
            # packsize = int(math.ceil(float(_rows) / 1024))
            # if packsize > 64:
            #     packsize = 64
            packsize = 64
            thread = LoaderThread(dlp_pool, _table, _csvfile, packsize, _rows, verbose)
            thread.start()
            load_threads.append(thread)
        ioprint('{0} load threads running ...'.format(len(load_threads)))
    ioprint('Waiting for all load threads to complete ...')
    errors = []
    for t in load_threads:
        ioprint('Waiting for {0} to load ...'.format(t.get_load_table()))
        try:
            t.join()
        except BaseException as be:
            errors.append(be)
    if errors:
        for e in errors:
            print(e)
    ioprint('Joined all threads.')
    rop_load_time = time.time() - rop_load_start_time
    ioprint('Loaded all files from {0} in {1} mSec'.format(rop_info_file, rop_load_time))
    ioprint('Loading of {0} finished, writing details ...'.format(rop_info_file))
    date_string = time.strftime("%Y-%m-%d-%H:%M")
    loadtimes = '{0}_loadtimes_{1}.csv'.format(rop_info_file, date_string)

    def format_line(frc, load_time, table, csv, start, end, dlp, total):
        return '{0},{1},{2},{3},{4},{5},{6},{7}\n'.format(frc, load_time, table, csv, start, end, dlp, total)

    if os.path.exists(loadtimes):
        os.remove(loadtimes)
    f = open(loadtimes, 'w')
    f.write(
        format_line('FLC', 'LOAD_TIME (Milliseconds)', 'TABLE', 'CSVFILE', 'START_TIME', 'END_TIME', 'DLP',
                    'Total Load Time (Seconds)'))
    f.write(format_line('0', '-', '-', '-', '-', '-', '-', rop_load_time))
    for t in load_threads:
        f.write(format_line(t.get_row_count(), t.get_load_time(), t.get_load_table(), t.get_load_file(),
                            '\'{0}\''.format(t.get_load_start_time()), '\'{0}\''.format(t.get_load_end_time()),
                            t.get_dlp_load_info(), '-'))
    f.close()
    ioprint('Load timings stored in {0}'.format(loadtimes))
    if errors:
        ioprint('There were errors in the loading, exiting')
        exit(3)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('--truncate', action='store_true', help='** Truncate ALL tables **')
    parser.add_option('--load', help='Load rops')
    parser.add_option('--sindex', default='1', help='Start loading from rop index')
    parser.add_option('--verbose', default=None, help='cmd:dlp or combination thereof')
    parser.add_option('--wait', default=False, action='store_true', help='If load=* when wait for files')
    parser.add_option('--cycle', default=False, action='store_true',
                      help='If load=* and there\' not more rop list files, restart from rop-1')
    parser.add_option('--dlp', dest='dlp_config', help='DLP configuration')
    (options, args) = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        exit()
    if not options.dlp_config:
        ioprint('No dlp config option!')
        parser.print_help()
        exit()
    if options.wait is True and options.cycle is True:
        ioprint('Options --cycle and --wait are mutually exclusive, use one or the other')
        exit(2)
    if not options.dlp_config:
        ioprint('Option --dlp not found!')
        exit(2)
    if options.verbose is not None:
        options.verbose = options.verbose.split(':')
    if options.truncate:
        truncate()
    if options.load:
        if not os.path.exists(options.dlp_config):
            ioprint('File {0} not found!'.format(options.dlp_config))
            exit(2)
        dlp_config = {}
        with open(options.dlp_config) as f:
            for line in f:
                line = line.strip()
                if len(line) == 0 or line.startswith('#'):
                    continue
                (key, val) = line.split('=', 1)
                dlp_config[key] = val
        if 'dlp_list' not in dlp_config:
            ioprint('No key \'dlp_list\' in {1}'.format(options.dlp_config))
            exit(3)
        DLP_CONFIG = {}
        for _dlp in dlp_config['dlp_list'].split(','):
            DLP_CONFIG[_dlp] = {}
            DLP_CONFIG[_dlp]['host'] = dlp_config['{0}.host'.format(_dlp)]
            DLP_CONFIG[_dlp]['user'] = dlp_config['{0}.user'.format(_dlp)]
            DLP_CONFIG[_dlp]['instances'] = dlp_config['{0}.instances'.format(_dlp)]
            DLP_CONFIG[_dlp]['license'] = dlp_config['{0}.license'.format(_dlp)]
            _key = '{0}.heap'.format(_dlp)
            if _key in dlp_config:
                DLP_CONFIG[_dlp]['heap'] = dlp_config[_key]
            else:
                DLP_CONFIG[_dlp]['heap'] = 320
                ioprint('No heap settings for {0}, defaulting to {1}'.format(_dlp, DLP_CONFIG[_dlp]['heap']))
            _key = '{0}.buffer'.format(_dlp)
            if _key in dlp_config:
                DLP_CONFIG[_dlp]['buffer'] = dlp_config[_key]
            else:
                DLP_CONFIG[_dlp]['buffer'] = 64
                ioprint('No buffer settings for {0}, defaulting to {1}'.format(_dlp, DLP_CONFIG[_dlp]['buffer']))
            _key = '{0}.cthreads'.format(_dlp)
            if _key in dlp_config:
                DLP_CONFIG[_dlp]['cthreads'] = dlp_config[_key]
            else:
                DLP_CONFIG[_dlp]['cthreads'] = 16
                ioprint('No compression threads settings for {0}, defaulting to {1}'.format(_dlp, DLP_CONFIG[_dlp][
                    'cthreads']))
            ioprint('DLP config {0}'.format(DLP_CONFIG[_dlp]))
        dlp_pool = Queue()
        for dlpc in DLP_CONFIG:
            for dlp_id in range(1, int(DLP_CONFIG[dlpc]['instances']) + 1):
                dlp_instance = '/opt/infobright/tools/distributed-load-processor_{0}/dataprocessor'.format(dlp_id)
                dlp_pool.put([DLP_CONFIG[dlpc], dlp_instance])
        try:
            if options.load == '*':
                _count = int(options.sindex)
                while True:
                    rop_info_file = '/var/datagen/rop-{0}.txt'.format(_count)
                    ioprint('Trying to load from {0}'.format(rop_info_file))
                    _count += 1
                    if not os.path.exists(rop_info_file):
                        if options.wait:
                            while not os.path.exists(rop_info_file):
                                ioprint('Waiting on {0}'.format(rop_info_file))
                                time.sleep(5)
                        elif options.cycle:
                            _count = 1
                            start_rop_info_file = '/var/datagen/rop-{0}.txt'.format(_count)
                            if not os.path.exists(start_rop_info_file):
                                ioprint('{0} not found, cant cycle back to start'.format(start_rop_info_file))
                                break
                            else:
                                ioprint('{0} not found, cycling back to start'.format(rop_info_file))
                                continue
                        else:
                            ioprint('No more files to load.')
                            break
                    load_rop(rop_info_file, dlp_pool, options.verbose)
            else:
                if not os.path.exists(options.load):
                    if options.wait:
                        while not os.path.exists(options.load):
                            ioprint('Waiting on {0}'.format(options.load))
                            time.sleep(5)
                    else:
                        ioprint('File {0} not found!'.format(options.load))
                        exit(3)
                while True:
                    load_rop(options.load, dlp_pool, options.verbose)
                    if options.cycle:
                        ioprint('Cycling back to {0}'.format(options.load))
                    else:
                        break
        except KeyboardInterrupt as e:
            pass