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
        success_count, warning_count, failed_count, skipped_count = 0, 0, 0, 0
        if not self.results:
            self.logger.info("No tables processed.")
        else:
            for r in self.results:
                if r['status'] == 'SUCCESS': success_count += 1
                elif r['status'] == 'WARNING': warning_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
                elif r['status'] == 'SKIPPED': skipped_count +=1
            for r in self.results:
                self.logger.info("{0} | {1} | {2}".format(r['table'], r['status'], r['message']))
            
            self.logger.info("Total: {0} | Success: {1} | Warning: {2} | Failed: {3} | Skipped: {4}".format(
                len(self.results), success_count, warning_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("Total: {0} | Success: {1} | Failed: {2} | Skipped: {3}".format(len(self.results), success_count, failed_count, skipped_count))
        print("Log File: {0}".format(log_path))
        print("="*80)

def setup_logging(log_dir, log_name="app", date_folder=None, timestamp=None):
    if date_folder:
        log_dir = os.path.join(log_dir, date_folder)
    if not os.path.exists(log_dir):
        try: os.makedirs(log_dir)
        except OSError: log_dir = '.'
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
    except Exception: pass
    return value

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class Config(object):
    def __init__(self, env_config_path, list_file_path, logger, date_folder=None, main_path=None):
        self.logger = logger

        # 1. Load Environment Config
        self.local_temp_dir = os.path.join(main_path, 'temp')
        self.nas_dest_base = os.path.join(main_path, 'output')
        self.log_dir = os.path.join(main_path, 'log')
        self.data_type_file_path = None
        self.th_col_config_table = 'prodgp.gpoffload.gp_list_th_col'

        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key, value = key.strip(), value.strip()
                        if key == 'local_temp_dir': self.local_temp_dir = value
                        elif key == 'nas_destination': self.nas_dest_base = value
                        elif key == 'log_dir': self.log_dir = value
                        elif key == 'data_type_file_path': self.data_type_file_path = value
                        elif key == 'th_col_config_table': self.th_col_config_table = value

            if self.local_temp_dir:
                self.local_temp_dir = os.path.join(self.local_temp_dir, date_folder)
                if not os.path.exists(self.local_temp_dir): os.makedirs(self.local_temp_dir)
            if self.nas_dest_base:
                self.nas_dest_base = os.path.join(self.nas_dest_base, date_folder)
        except Exception as e:
            self.logger.error("Failed to load env config: {0}".format(e))
            raise

        # 2. Fetch Thai Column Config from Greenplum DB
        self.th_cols_map ={}
        self.logger.info("Fetching Thai column config using table format: {0}".format(self.th_col_config_table))

        try:
            db_part, schema_table_part = self.th_col_config_table.split('.', 1)
        except ValueError:
            self.logger.error("Invalid format for th_col_config_table. Expected DB.SCHEMA.TABLE (e.g., prodgp.gpoffload.gp_list_th_col)")
            raise ValueError("Invalid configuration for th_col_config_table")
        sql = "SELECT database_name, original_table_name, th_column_name FROM {0} WHERE upper(active_flag) = 'Y';".format(schema_table_part)
        cmd = ['psql', '-d', db_part, '-A', '-t', '-F', '|', '-c', sql]

        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                self.logger.warning("Failed to fetch Thai columns from DB. Error: {0}".format(stderr))
            else:
                for line in stdout.splitlines():
                    line = line.strip()
                    if not line: continue
                    parts = line.split('|')
                    if len(parts) == 3:
                        db_name, sch_tbl, col = parts
                        dict_key = "{0}|{1}".format(db_name.strip().lower(), sch_tbl.strip().lower())
                        if dict_key not in self.th_cols_map:
                            self.th_cols_map[dict_key] = []
                        self.th_cols_map[dict_key].append(col.strip().lower())
                self.logger.info("Loaded Thai column config for {0} tables from DB".format(len(self.th_cols_map)))
        except Exception as e:
            self.logger.error("Exception fetching Thai config: {0}".format(e))
        
        # 3. Load Execution List
        self.execution_list = []
        self.logger.info("Loading list file: {0}".format(list_file_path))
        try:
            with open(list_file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    try:
                        # Format: DB|Schema.Table|KEY_COLS
                        parts = line.split('|')
                        if len(parts) >= 3:
                            db_part = parts[0].strip()
                            tbl_part = parts[1].strip()
                            key_cols = parts[2].strip()
                            sch_part, real_tbl = tbl_part.split('.')
                            self.execution_list.append({
                                'db': db_part,
                                'schema': sch_part,
                                'table': real_tbl,
                                'key_cols': key_cols
                            })
                    except ValueError:
                        self.logger.warning("Skipping invalid line: {0}".format(line))
        except Exception as e:
            self.logger.error("Failed to load list file: {0}".format(e))

# ==============================================================================
# 3. Handlers & Builders
# ==============================================================================

class QueryBuilder(object):
    def __init__(self, temp_dir, logger, global_ts):
        self.temp_dir = temp_dir
        self.logger = logger
        self.global_ts = global_ts

    def build_reconcile_query(self, db, schema, table, key_cols_str, all_cols, th_cols, exclude_content):
        try:
            def format_col(col_name):
                # Apply Thai Encoding if needed (case-insensitive check)
                if col_name.lower() in th_cols:
                    return "COALESCE(convert_from(convert_to({0}, 'utf8'), 'win874')::text, '')".format(col_name)
                else:
                    return "COALESCE({0}::text, '')".format(col_name)
            
            # 1. Resolve Key Columns
            if key_cols_str.upper() == 'ALL':
                k_cols = all_cols
            else:
                k_cols = [c.strip() for c in key_cols_str.split(',') if c.strip()]

            for k in k_cols:
                if k.lower() not in [c.lower() for c in all_cols]:
                    raise ValueError("Key column '{0}' not found in table '{1}.{2}'".format(k, schema, table))
                
            # 2. Build Expressions
            delimiter = " || '~|~' || "

            key_exprs = [format_col(c) for c in k_cols]
            key_concat = delimiter.join(key_exprs)

            content_exprs = [format_col(c) for c in all_cols]
            content_concat = delimiter.join(content_exprs)

            # 3. Assemble Final SQL
            sql = "SELECT "
            sql += "md5({0}) AS hash_key, ".format(key_concat)
            sql += "{0} AS key, ".format(key_concat)
            sql += "md5({0}) AS hash_content, ".format(content_concat)

            if exclude_content:
                sql += "'' AS content "
            else:
                sql += "{0} AS content ".format(content_concat)

            sql += "FROM {0}.{1};".format(schema, table)

            # 4. Save to file (for debugging/logging)
            filename = "query_reconcile_{0}_{1}_{2}.sql".format(db, table, self.global_ts)
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write(sql)

            return sql
        except Exception as e:
            self.logger.error("Error building query for {0}.{1}: {2}".format(schema, table, e))
            raise

class ShellHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def run_psql(self, sql, output_path, db_name=None):
        cmd = ['psql', '-A', '-t', '-c', sql, '-o', output_path]
        cmd.extend(['-F', '|'])

        if db_name: cmd.extend(['-d', db_name])

        self.logger.info("Executing PSQL... -> Output: {0}".format(output_path))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError("PSQL execution failed. Error: {0}".format(stderr))
        except Exception as e:
            raise

class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def compress_file(self, file_path):
        self.logger.info("Compressing file: {0}".format(file_path))
        try:
            subprocess.check_call(['gzip', '-f', file_path])
            return file_path + ".gz"
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to compress file: {0}".format(e))
            raise

    def copy_to_nas(self, src, dest_dir):
        if not os.path.exists(dest_dir):
            try: os.makedirs(dest_dir)
            except OSError: pass
        self.logger.info("Copying file to NAS: {0}".format(dest_dir))
        shutil.copy2(src, dest_dir)

# ==============================================================================
# 4. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, builder, shell, file_h, tracker, logger, global_ts, args):
        threading.Thread.__init__(self)
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.builder = builder
        self.shell = shell
        self.file_h = file_h
        self.tracker = tracker
        self.logger = logger
        self.global_ts = global_ts
        self.args = args
        self.daemon = True
    
    def _get_columns_from_csv(self, db, schema, table):
        if not self.config.data_type_file_path:
            raise ValueError("data_type_file_path is not configured.")
        
        csv_path = os.path.join(self.config.data_type_file_path, db, schema, "{0}.csv".format(table))
        if not os.path.exists(csv_path):
            raise IOError("Data type file NOT FOUND: {0}".format(csv_path))
        
        columns = []
        with open(csv_path, 'r') as cf:
            reader = csv.DictReader(cf, delimiter='|')
            for row in reader:
                gp_col = row.get('gp_column_nm')
                if gp_col:
                    columns.append(gp_col.strip())
        
        if not columns:
            raise ValueError("No columns found in {0}".format(csv_path))
        return columns
    
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
                key_cols_str = task['key_cols']

                full_name = "{0}.{1}.{2}".format(db, schema, table)
                dict_key = "{0}|{1}.{2}".format(db.lower(), schema.lower(), table.lower())

                self.tracker.update_worker_status(self.name, "[BUSY] {0}.{1}".format(schema, table))
                start_t = time.time()

                all_cols = self._get_columns_from_csv(db, schema, table)
                th_cols = self.config.th_cols_map.get(dict_key, [])

                sql = self.builder.build_reconcile_query(
                    db, schema, table, key_cols_str, all_cols, th_cols, self.args.exclude_content
                )

                output_filename = "reconcile_{0}_{1}_{2}.txt".format(db, table, self.global_ts)
                local_path = os.path.join(self.config.local_temp_dir, output_filename)

                self.shell.run_psql(sql, local_path, db)

                final_file_path = local_path
                if self.args.compress:
                    final_file_path = self.file_h.compress_file(local_path)

                self.file_h.copy_to_nas(final_file_path, self.config.nas_dest_base)

                self.tracker.add_result(full_name, "SUCCESS", "-")
                self.tracker.log_step(full_name, time.time() - start_t)

            except Exception as e:
                self.tracker.add_result(full_name, "FAILED", str(e))
                self.logger.error("Worker {0} Failed {1}: {2}".format(self.name, full_name, e))
            finally:
                self.queue.task_done()

class MonitorThread(threading.Thread):
    def __init__(self, tracker, num_workers, log_path):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.num_workers = num_workers
        self.log_path = log_path
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard()

    def print_dashboard(self):
        comp, total = self.tracker.get_progress()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        lines = ["============================================================"]
        lines.append(" GP RECONCILE EXTRACTOR (Python 2.7 Parallel) ")
        lines.append("============================================================")
        lines.append(" Progress: {0}/{1} ({2:.2f}%) | Elapsed: {3:.0f}s".format(comp, total, pct, elapsed))
        lines.append("-" * 60)

        for w_name in sorted(self.tracker.worker_status.keys()):
            lines.append(" {0} : {1}".format(w_name, self.tracker.worker_status.get(w_name))[:79]) 
        
        lines.append("-" * 60)
        lines.append(" Log: {0}".format(self.log_path))

        if not self.first_print:
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        sys.stdout.write("\n".join([line + "\033[K" for line in lines]) + "\n")
        sys.stdout.flush()

# ==============================================================================
# 5. Main Execution
# ==============================================================================
def main():
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Greenplum Data Reconcile Export Tool')
    parser.add_argument('--env', default='env_config.txt', help='Env config file')
    parser.add_argument('--list', default='list_table.txt', help='List of tables file')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel workers')
    
    parser.add_argument('--compress', action='store_true', help='Enable GZIP compression before copying to NAS')
    parser.add_argument('--exclude_content', action='store_true', help='Output empty string for the content column')

    args = parser.parse_args()

    def resolve_path(input_path):
        return input_path if os.path.isabs(input_path) else os.path.join(main_path, 'config', input_path)

    args.env = resolve_path(args.env)
    args.list = resolve_path(args.list)

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")

    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else os.path.join(main_path, 'log')

    logger, log_path = setup_logging(final_log_dir, 'gp_reconcile', global_date_folder, global_ts)
    
    logger.info("Started with concurrency: {0}".format(args.concurrency))
    logger.info("Option Compress: {0}".format(args.compress))
    logger.info("Option Exclude Content: {0}".format(args.exclude_content))

    tracker = ProcessTracker(logger)
    
    config = Config(args.env, args.list, logger, global_date_folder, main_path)
    builder = QueryBuilder(config.local_temp_dir, logger, global_ts)
    shell = ShellHandler(logger)
    file_h = FileHandler(logger)
    
    logger.info("Thai config table loaded from env: {0}".format(config.th_col_config_table))

    job_queue = Queue.Queue()
    for task in config.execution_list:
        job_queue.put(task)
    tracker.set_total_tasks(len(config.execution_list))

    workers = []
    for i in range(args.concurrency):
        w = Worker(i+1, job_queue, config, builder, shell, file_h, tracker, logger, global_ts, args)
        workers.append(w)
        w.start()

    monitor = MonitorThread(tracker, args.concurrency, log_path)
    monitor.start()

    try:
        while tracker.completed_task < tracker.total_task:
            if not any(w.is_alive() for w in workers): break
            time.sleep(1)
        for w in workers: w.join()
    except KeyboardInterrupt:
        logger.warning("Aborted by user.")
    finally:
        monitor.stop()
        monitor.join()
        tracker.print_summary(log_path)

if __name__ == "__main__":
    main()