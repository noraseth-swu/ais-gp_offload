#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import logging
import subprocess
import shutil
import time
import argparse
import threading
import Queue
import uuid
from datetime import datetime

# ==============================================================================
# 1. Utilities: Logger & ProcessTracker
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, logger):
        self.logger = logger
        self.lock = threading.Lock()
        self.results = []
        self.worker_status = {}
        self.total_task = 0
        self.completed_task = 0
        self.start_time = time.time()

    def set_total_tasks(self, total):
        self.total_task = total

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, message="-"):
        with self.lock:
            self.results.append({
                'table': table_name,
                'status': status,
                'message': str(message).replace('\n', ' ')
            })
            self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
    
    def log_step(self, step_name, duration):
        self.logger.info("Step: {0} | Duration: {1:.2f}s".format(step_name, duration))

    def print_summary(self, log_path):
        self.logger.info("="*80)
        self.logger.info("TABLE EXECUTION SUMMARY")
        self.logger.info("="*80)

        success_count = 0
        failed_count = 0
        skipped_count = 0

        if not self.results:
            self.logger.info("No tables processed.")
        else:
            h_table = "Table Name"
            h_status = "Status"
            h_msg = "Error / Remark"

            max_w_table = len(h_table)
            max_w_status = len(h_status)

            for r in self.results:
                if len(r['table']) > max_w_table: max_w_table = len(r['table'])
                if len(r['status']) > max_w_status: max_w_status = len(r['status'])

                if r['status'] == 'SUCCESS': success_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
                elif r['status'] == 'SKIPPED': skipped_count +=1
                       
            
            w_table = max_w_table + 2
            w_status = max_w_status + 2

            row_fmt = "{0:<{wt}} | {1:<{ws}} | {2}"

            header_line = row_fmt.format(h_table, h_status, h_msg, wt=w_table, ws=w_status)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 50: sep_line = "-" * 50

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            for r in self.results:
                self.logger.info(row_fmt.format(r['table'], r['status'], r['message'], wt=w_table, ws=w_status))
            
            self.logger.info(sep_line)
            self.logger.info("Total: {0} | Success: {1} | Failed: {2} | Skipped: {3}".format(
                len(self.results), success_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Print Summary to Console (Last view)
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total: {0}".format(len(self.results)))
        print("Success: {0}".format(success_count))
        print("Failed:  {0}".format(failed_count))
        print("Skipped: {0}".format(skipped_count))
        print("Log File: {0}".format(log_path))
        print("="*80)

def setup_logging(log_dir, log_name="app"):
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Using current directory. Error: {1}".format(log_dir, e))
            log_dir = '.'

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))

    logger = logging.getLogger("GreenplumBatch")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger, log_file

def peek_env_config(env_path, key_to_find):
    value = None
    try:
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        k, v = line.split('=', 1)
                        if k.strip() == key_to_find:
                            value = v.strip()
    except Exception:
        pass
    return value

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class Config(object):
    def __init__(self, env_config_path, master_config_path, list_file_path, cli_tables, logger):
        self.logger = logger
        
        # 1. Load Environment Config
        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.local_temp_dir = './temp'
        self.nas_dest_base = None
        self.log_dir = None
        
        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue

                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        if key == 'local_temp_dir':
                            self.local_temp_dir = value
                        elif key == 'nas_destination':
                            self.nas_dest_base = value
                        elif key == 'log_dir':
                            self.log_dir = value
                
            # Create temp dir if not exists
            if not os.path.exists(self.local_temp_dir):
                os.makedirs(self.local_temp_dir)
        except Exception as e:
            self.logger.error("Failed to load environment config: {0}".format(e))
            raise

        # 2. Load Master Config (Lookup Dictionary)
        # Key: (db_name, schema, table) -> Value: row dict
        self.master_data = {}
        self.logger.info("Loading master config: {0}".format(master_config_path))
        try:
            with open(master_config_path, 'r') as f:
                reader = csv.reader(f, delimiter='|')
                for line in reader:
                    # Format: db|schema|table|partition|cols|where|...
                    if len(line) < 6: continue
                    db, sch, tbl, part, cols, where = [x.strip() for x in line]
                    key = (db, sch, tbl)
                    self.master_data[key] = {
                        'partition': part, 
                        'cols': cols,
                        'where': where
                    }
        except Exception as e:
            self.logger.error("Failed to load master config: {0}".format(e))
            raise

        # 3. Determine Execution List
        self.execution_list = []
        if cli_tables:
            # Case 1: Use CLI Arguments
            self.logger.info("Using CLI arguments for table list.")
            # Expect format: DB|Schema.Table,DB2|Schema.Table
            tables = cli_tables.split(',')
            for t in tables:
                try:
                    # Parse DB|Schema.Table
                    db_part, tbl_part = t.split('|')
                    sch_part, real_tbl = tbl_part.split('.')
                    self.execution_list.append({
                        'db': db_part.strip(),
                        'schema': sch_part.strip(),
                        'table': real_tbl.strip()
                    })
                except ValueError:
                    self.logger.error("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
        else:
            # Case 2: Use List File
            self.logger.info("Using list file: {0}".format(list_file_path))
            try:
                with open(list_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): continue
                        try:
                            # Format: DB|Schema.Table
                            db_part, tbl_part = line.split('|')
                            sch_part, real_tbl = tbl_part.split('.')
                            self.execution_list.append({
                                'db': db_part.strip(),
                                'schema': sch_part.strip(),
                                'table': real_tbl.strip()
                            })
                        except ValueError:
                            self.logger.warning("Skipping invalid line in list file: {0}".format(line))
            except Exception as e:
                self.logger.error("Failed to load list file: {0}".format(e))

# ==============================================================================
# 3. Handler Classes
# ==============================================================================

class QueryBuilder(object):
    def __init__(self, temp_dir, logger):
        self.temp_dir = temp_dir
        self.logger = logger

    def build_and_save_query(self, db, schema, table, partition, cols_str, where):
        """
        Constructs SQL to match output requirement:
        db|schema|table_<part>|<count>|col1:sum(col1),...
        """
        try:
            cols = []

            if cols_str and cols_str.strip():
                cols = [c.strip() for c in cols_str.split(',') if c.strip()]
            
            # Logic for Table Name in Output (Partition vs Non-Partition)
            if partition and partition.lower() != 'none' and partition != '':
                tbl_display_logic = "'{0}|{1}|{2}_{3}'".format(db, schema, table, partition)
            else:
                tbl_display_logic = "'{0}|{1}|{2}'".format(db, schema, table)
            if cols:
                # Build Sum Columns Logic: 'colname:' || sum(col)
                sum_parts = []
                for col in cols:
                    part = "'{0}:' || COALESCE(SUM({0}), 0)::text".format(col)
                    sum_parts.append(part)
                sum_logic = " || ',' || ".join(sum_parts)

                # Final SQL
                sql = "SELECT {0} || '|' || COUNT(*)::text || '|' || {1} FROM {2}.{3}".format(
                    tbl_display_logic, sum_logic, schema, table
                )
            else:
                # Case B: No columns -> Count Only
                sql = "SELECT {0} || '|' || COUNT(*)::text || '|' FROM {1}.{2}".format(
                    tbl_display_logic, schema, table
                )

            if where and where.strip():
                sql += " WHERE {0}".format(where)

            sql += ";"

            # Save SQL to temp file for history
            unique_id = str(uuid.uuid4())[:8]
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = "query_{0}_{1}_{2}_{3}.sql".format(db, table, ts, unique_id)
            filepath = os.path.join(self.temp_dir, filename)

            with open(filepath, 'w') as f:
                f.write(sql)

            self.logger.info("Generated SQL saved to: {0}".format(filepath))
            return sql

        except Exception as e:
            self.logger.error("Error building query: {0}".format(e))
            raise

class ShellHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def run_psql(self, sql, output_path, db_name=None):
        cmd = ['psql', '-A', '-t', '-c', sql, '-o', output_path]

        if db_name: cmd.extend(['-d', db_name])

        self.logger.info("Executing PSQL... (DB: {0})".format(db_name or 'Default'))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError("PSQL execution failed. RC: {0}. Error: {1}".format(process.returncode, stderr))
            if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
                pass 
        except Exception as e:
            raise

class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger
    def copy_to_nas(self, src, dest_dir):
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError:
                pass
        shutil.copy2(src, dest_dir)

# ==============================================================================
# 3. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, builder, shell, file_h, tracker, logger):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.builder = builder
        self.shell = shell
        self.file_h = file_h
        self.tracker = tracker
        self.logger = logger
        self.daemon = True

    def run(self):
        while True:
            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            try:
                db = task['db']
                schema = task['schema']
                table = task['table']

                full_name = "{0}.{1}.{2}".format(db, schema, table)
                short_name = "{0}.{1}".format(schema, table)

                # --- RETRY LOOP ---
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.tracker.update_worker_status(self.name, "[BUSY] {0} (Try {1}/{2})".format(short_name, attempt+1, max_retries))
                        start_t = time.time()

                        # 1. Config Lookup
                        key = (db, schema, table)
                        master_info = self.config.master_data.get(key)

                        if not master_info:
                            self.tracker.add_result(full_name, "SKIPPED", "No Config")
                            self.logger.warning("Skipping {0}: No Config".format(full_name))
                            break

                        # 2. Build SQL
                        sql = self.builder.build_and_save_query(
                            db, schema, table,
                            master_info['partition'],
                            master_info['cols'],
                            master_info['where']
                        )

                        # 3. PSQL
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        unique_id = str(uuid.uuid4())[:8]
                        output_filename = "{0}_{1}_{2}_{3}.txt".format(db, table, ts, unique_id)
                        local_path = os.path.join(self.config.local_temp_dir, output_filename)

                        self.shell.run_psql(sql, local_path, db)

                        # 4. Copy
                        self.file_h.copy_to_nas(local_path, self.config.nas_dest_base)

                        # Success
                        self.tracker.add_result(full_name, "SUCCESS", "-")
                        self.tracker.log_step(full_name, time.time() - start_t)
                        break # Break retry loop on success

                    except Exception as e:
                        safe_err = repr(e)
                        if attempt < max_retries - 1:
                            self.logger.warning("Worker {0} failed on {1}. Retrying ... Error: {2}".format(self.name, full_name, safe_err))
                            time.sleep(3)
                        else:
                            # Final Failure
                            self.tracker.add_result(full_name, "FAILED", str(safe_err))
                            self.logger.error("Failed {0}: {1}".format(full_name, safe_err))
            
            except Exception as outer_e:
                safe_err = repr(outer_e)
                self.logger.error("Worker {0} FATAL CRASH on a task. Error: {1}".format(self.name, safe_err))
                self.tracker.add_result(str(task.get('table', 'UNKNOWN')), "CRASHED", safe_err)
            finally:
                # Mark task done in queue
                self.queue.task_done()

class MonitorThread(threading.Thread):
    def __init__(self, tracker, num_workers):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.num_workers = num_workers
        self.stop_event = threading.Event()
        self.daemon = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard()

    def print_dashboard(self):
        os.system('cls' if os.name == 'nt' else 'clear')

        comp, total = self.tracker.get_progress()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        print("="*60)
        print(" GREENPLUM EXPORT MONITOR (Python 2.7 Parallel) ")
        print("="*60)
        print(" Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct))
        print(" Elapsed : {0:.0f}s".format(elapsed))
        print("-" * 60)

        # Print Status of each worker
        worker = sorted(self.tracker.worker_status.keys())
        for w_name in worker:
            status = self.tracker.worker_status.get(w_name, "Initializing...")
            print(" {0} : {1}".format(w_name, status))
        
        print("-" * 60)
        print(" Press Ctl+C to abort.")

# ==============================================================================
# 4. Main Job Class
# ==============================================================================
class GreenplumExportJob(object):
    def __init__(self, args, logger, log_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.tracker = ProcessTracker(logger)

        # Init Helpers
        self.config = Config(args.env, args.master, args.list, args.table_name, logger)
        self.builder = QueryBuilder(self.config.local_temp_dir, logger)
        self.shell = ShellHandler(logger)
        self.file_h = FileHandler(logger)

        # Setup Queue
        self.job_queue = Queue.Queue()
        for task in self.config.execution_list:
            self.job_queue.put(task)

        self.tracker.set_total_tasks(len(self.config.execution_list))
        self.logger.info("Loaded {0} tasks into queue.".format(len(self.config.execution_list)))

    def run(self):
        num_workers = int(self.args.concurrency)
        self.logger.info("Starting {0} workers...".format(num_workers))

        workers = []
        # Create & Start Workers
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.builder, self.shell, self.file_h, self.tracker, self.logger)
            workers.append(w)
            w.start()

        # Start Monitor
        monitor = MonitorThread(self.tracker, num_workers)
        monitor.start()

        # Wait for Queue to be empty
        try:
            while any(w.is_alive() for w in workers):
                time.sleep(0.5)
                if self.job_queue.empty() and all(not w.is_alive() for w in workers):
                    break
        except KeyboardInterrupt:
            self.logger.warning("Keyboard Interrupt! Stopping...")
        finally:
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.log_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Greenplum Data Export Tool')
    parser.add_argument('--env', default='config.txt', help='Path to environment config text file')
    parser.add_argument('--master', default='config_master.txt', help='Path to master config pipe-delimited')
    parser.add_argument('--list', default='list_table.txt', help='Path to list of tables to run')
    parser.add_argument('--table_name', help='Optional: Specific tables to run (DB|Schema.Table) comma separated')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel workers (Default: 4)')

    args = parser.parse_args()

    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    default_log_dir = os.path.join(current_script_dir, 'logs')
    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else default_log_dir

    logger, log_path = setup_logging(final_log_dir, 'gp_export')
    logger.info("Started with concurrency: {0}".format(args.concurrency))

    try:
        job = GreenplumExportJob(args, logger, log_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e))
        sys.exit(1)