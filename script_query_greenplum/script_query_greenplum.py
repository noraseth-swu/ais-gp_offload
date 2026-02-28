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
        warning_count = 0
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
                elif r['status'] == 'WARNING': warning_count += 1
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
            self.logger.info("Total: {0} | Success: {1} | Warning: {2} | Failed: {3} | Skipped: {4}".format(
                len(self.results), success_count, warning_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Print Summary to Console (Last view)
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total: {0}".format(len(self.results)))
        print("Success: {0}".format(success_count))
        print("Warning: {0}".format(warning_count))
        print("Failed:  {0}".format(failed_count))
        print("Skipped: {0}".format(skipped_count))
        print("Log File: {0}".format(log_path))
        print("="*80)

def setup_logging(log_dir, log_name="app", date_folder=None, timestamp=None):
    if date_folder:
        log_dir = os.path.join(log_dir, date_folder)

    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Using current directory. Error: {1}".format(log_dir, e))
            log_dir = '.'

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
    def __init__(self, env_config_path, master_config_path, list_file_path, cli_tables, logger, date_folder = None, main_path=None):
        self.logger = logger
        
        # 1. Load Environment Config
        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.local_temp_dir = os.path.join(main_path, 'temp')
        self.nas_dest_base = os.path.join(main_path, 'output')
        self.log_dir = os.path.join(main_path, 'log')
        self.data_type_file_path = None
        
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
                        elif key == 'data_type_file_path':
                            self.data_type_file_path = value
                
            # Create temp dir if not exists
            if self.local_temp_dir:
                self.local_temp_dir = os.path.join(self.local_temp_dir, date_folder)
                if not os.path.exists(self.local_temp_dir):
                    os.makedirs(self.local_temp_dir)

            if self.nas_dest_base:
                self.nas_dest_base = os.path.join(self.nas_dest_base, date_folder)
            
            self.logger.info("Resolved local_temp_dir: {0}".format(self.local_temp_dir))
            self.logger.info("Resolved nas_dest_base: {0}".format(self.nas_dest_base))
            self.logger.info("Resolved log_dir: {0}".format(self.log_dir))
            self.logger.info("Resolved data_type_file_path: {0}".format(self.data_type_file_path))

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
                    # Format: DB | SCHEMA | table | sum_cols | where
                    if len(line) < 5: continue
                    db, sch, tbl, sum_cols, where = [x.strip() for x in line[:5]]
                    key = (db, sch, tbl)
                    self.master_data[key] = {
                        'sum_cols': sum_cols, 
                        'where': where
                    }
            self.logger.info("Loaded {0} tables from master config.".format(len(self.master_data)))
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
    def __init__(self, temp_dir, logger, global_ts):
        self.temp_dir = temp_dir
        self.logger = logger
        self.global_ts = global_ts

    def _parse_aggregate_cols(self, cols_data, agg_func):
        parts = []
        if not cols_data:
            return parts
        
        if isinstance(cols_data, str):
            if cols_data.strip().lower() == 'none' or not cols_data.strip():
                return parts
            cols = [c.strip() for c in cols_data.split(',') if c.strip()]
        elif isinstance(cols_data, list):
            cols = cols_data
        else:
            cols = []

        for col in cols:
            if agg_func == 'SUM':
                parts.append("'{1}({0}):' || COALESCE(SUM({0}), 0)::text".format(col, agg_func))
            elif agg_func in ['MIN', 'MAX']:
                parts.append("'{1}({0}):' || COALESCE({1}({0})::text, '')".format(col, agg_func))
        return parts

    def build_and_save_query(self, db, schema, table, sum_cols_str, min_cols_list, max_cols_list, where, missing_flag):
        try:
            # DB|SCHEMA|TABLE
            tbl_display_logic = "'{0}|{1}|{2}'".format(db, schema, table)

            metric_parts = []
            metric_parts.extend(self._parse_aggregate_cols(sum_cols_str, 'SUM'))
            metric_parts.extend(self._parse_aggregate_cols(min_cols_list, 'MIN'))
            metric_parts.extend(self._parse_aggregate_cols(max_cols_list, 'MAX'))
            
            if missing_flag:
                metric_parts.append("'MIN_MAX_STATUS: DATA_TYPE_FILE_MISSING'")

            if metric_parts:
                metrics_logic = " || ',' || ".join(metric_parts)
                sql = "SELECT {0} || '|' || COUNT(*)::text || '|' || {1} FROM {2}.{3}".format(
                    tbl_display_logic, metrics_logic, schema, table
                )
            else:
                sql = "SELECT {0} || '|' || COUNT(*)::text || '|' FROM {1}.{2}".format(
                    tbl_display_logic, schema, table
                )

            if where and where.strip() and where.strip().lower() != 'none':
                sql += " WHERE {0}".format(where)

            sql += ";"

            filename = "query_{0}_{1}_{2}.sql".format(db, table, self.global_ts)
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

        self.logger.info("Executing PSQL... (DB: {0}) -> Output: {1}".format(db_name or 'Default', output_path))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError("PSQL execution failed. RC: {0}. Error: {1}".format(process.returncode, stderr))
            if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
                self.logger.warning("PSQL Executed but output file is missing or 0 bytes: {0}".format(output_path))
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
        self.logger.info("Copying file from {0} to NAS: {1}".format(src, dest_dir))
        shutil.copy2(src, dest_dir)

# ==============================================================================
# 3. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, builder, shell, file_h, tracker, logger, global_ts):
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
        self.global_ts = global_ts
        self.daemon = True

    def _is_min_max_candidate(self, gp_dt, ext_dt):
        gp_base = str(gp_dt).split('(')[0].strip().lower()
        ext_base = str(ext_dt).split('(')[0].strip().lower()

        exclude_keywords = [
            'json', 'xml', 
            'geometry', 'geography', 'box', 'point', 'line', 'lseg', 'path', 'polygon', 'circle'
        ]

        if any(keyword in gp_base for keyword in exclude_keywords):
            return False
        
        valid_gp_types = [
            'character varying', 'varchar', 'char', 'text', 'string', 
            'date', 'time', 'timestamp', 
            'int', 'integer', 'bigint', 'smallint', 
            'numeric', 'decimal', 'float', 'real', 'double precision'
        ]

        valid_ext_types = ['text', 'character varying', 'varchar', 'string', 'char']

        is_gp_ok = any(t in gp_base for t in valid_gp_types)
        is_ext_ok = any(t in ext_base for t in valid_ext_types)

        return is_gp_ok and is_ext_ok

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

                        self.logger.info("Worker {0} started processing table: {1} (Attempt {2})".format(self.name, full_name, attempt+1))

                        # 1. Config Lookup
                        key = (db, schema, table)
                        master_info = self.config.master_data.get(key)

                        if not master_info:
                            self.tracker.add_result(full_name, "SKIPPED", "No Config")
                            self.logger.warning("Worker {0} Skipping {1}: No Config found in master_data".format(self.name, full_name))
                            break

                        min_cols = []
                        max_cols = []
                        missing_flag = False

                        csv_path = None
                        if self.config.data_type_file_path:
                            csv_path = os.path.join(self.config.data_type_file_path, db, schema, "{0}.csv".format(table))
                        
                        if csv_path:
                            self.logger.info("Worker {0} looking for data type mapping file at: {1}".format(self.name, csv_path))
                            if os.path.exists(csv_path):
                                self.logger.info("Worker {0} found data type mapping file: {1}".format(self.name, csv_path))
                                try:
                                    with open(csv_path, 'r') as cf:
                                        reader = csv.DictReader(cf, delimiter='|')
                                        for row in reader:
                                            gp_col = row.get('gp_column_nm')
                                            gp_dt = row.get('gp_datatype', '')
                                            ext_dt = row.get('ext_datatype', '')
                                            
                                            if gp_col and self._is_min_max_candidate(gp_dt, ext_dt):
                                                min_cols.append(gp_col.strip())
                                                max_cols.append(gp_col.strip())
                                    self.logger.info("Worker {0} identified {1} MIN/MAX columns for {2}".format(self.name, len(min_cols), full_name))
                                except Exception as e:
                                    self.logger.warning("Worker {0} error reading data type file {1}: {2}".format(self.name, csv_path, e))
                                    missing_flag = True
                            else:
                                self.logger.warning("Worker {0} Data type mapping file NOT FOUND: {1}".format(self.name, csv_path))
                                missing_flag = True
                        else:
                            self.logger.warning("Worker {0} data_type_file_path is None. Skipping MIN/MAX search for {1}".format(self.name, full_name))
                            missing_flag = True

                        # 2. Build SQL
                        sql = self.builder.build_and_save_query(
                            db, 
                            schema, 
                            table, 
                            master_info['sum_cols'],
                            min_cols,
                            max_cols,
                            master_info['where'],
                            missing_flag
                        )

                        # 3. PSQL
                        output_filename = "{0}_{1}_{2}.txt".format(db, table, self.global_ts)
                        local_path = os.path.join(self.config.local_temp_dir, output_filename)

                        self.shell.run_psql(sql, local_path, db)

                        # 4. Copy
                        self.file_h.copy_to_nas(local_path, self.config.nas_dest_base)

                        # Success or Warning
                        if missing_flag:
                            warning_msg = "Count/Sum successful, unable to process min/max because the data type mapping file was not found."
                            self.tracker.add_result(full_name, "WARNING", warning_msg)
                        else:
                            self.tracker.add_result(full_name, "SUCCESS", "-")

                        self.tracker.log_step(full_name, time.time() - start_t)
                        self.logger.info("Worker {0} completed table: {1}".format(self.name, full_name))
                        break # Break retry loop on success

                    except Exception as e:
                        safe_err = repr(e)
                        if attempt < max_retries - 1:
                            self.logger.warning("Worker {0} failed on {1}. Retrying ... Error: {2}".format(self.name, full_name, safe_err))
                            time.sleep(3)
                        else:
                            # Final Failure
                            self.tracker.add_result(full_name, "FAILED", str(safe_err))
                            self.logger.error("Worker {0} Failed {1}: {2}".format(self.name, full_name, safe_err))
            
            except Exception as outer_e:
                safe_err = repr(outer_e)
                self.logger.error("Worker {0} FATAL CRASH on a task. Error: {1}".format(self.name, safe_err))
                self.tracker.add_result(str(task.get('table', 'UNKNOWN')), "CRASHED", safe_err)
            finally:
                # Mark task done in queue
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

        lines = []
        lines.append("============================================================")
        lines.append(" GREENPLUM EXPORT MONITOR (Python 2.7 Parallel) ")
        lines.append("============================================================")
        lines.append(" Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct))
        lines.append(" Elapsed : {0:.0f}s".format(elapsed))
        lines.append("-" * 60)

        workers = sorted(self.tracker.worker_status.keys())
        for w_name in workers:
            status = self.tracker.worker_status.get(w_name, "Initializing...")
            line_str = " {0} : {1}".format(w_name, status)
            lines.append(line_str[:79]) 
        
        lines.append("-" * 60)
        lines.append(" Log File: {0}".format(self.log_path))
        lines.append(" Press Ctrl+C to abort.")

        if not self.first_print:
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        
        sys.stdout.write(output)
        sys.stdout.flush()

# ==============================================================================
# 4. Main Job Class
# ==============================================================================
class GreenplumExportJob(object):
    def __init__(self, args, logger, log_path, global_date_folder, global_ts, main_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.global_ts = global_ts
        self.tracker = ProcessTracker(logger)

        # Init Helpers
        self.config = Config(args.env, args.master, args.list, args.table_name, logger, global_date_folder, main_path)
        self.builder = QueryBuilder(self.config.local_temp_dir, logger, self.global_ts)
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
            w = Worker(i+1, self.job_queue, self.config, self.builder, self.shell, self.file_h, self.tracker, self.logger, self.global_ts)
            workers.append(w)
            w.start()

        # Start Monitor
        monitor = MonitorThread(self.tracker, num_workers, self.log_path)
        monitor.start()

        # Wait for Queue to be empty
        try:
            while self.tracker.completed_task < self.tracker.total_task:
                if not any(w.is_alive() for w in workers):
                    self.logger.error("All workers died unexpectedly! Aborting wait loop.")
                    break
                time.sleep(1)
            for w in workers:
                w.join()
        except KeyboardInterrupt:
            sys.stdout.write("\n\n>>> KEYBOARD INTERRUPT DETECTED! ABORTING SCRIPT... <<<\n\n")
            sys.stdout.flush()
            self.logger.warning("Keyboard Interrupt! User aborted the script.")
        finally:
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.log_path)

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Greenplum Data Export Tool')
    parser.add_argument('--env', default='env_config.txt', help='Name of env config file')
    parser.add_argument('--master', default='config_master.txt', help='Name of master config file')
    parser.add_argument('--list', default='list_table.txt', help='Name of list of tables file')
    parser.add_argument('--table_name', help='Optional: Specific tables to run (DB|Schema.Table)')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel workers (Default: 4)')

    args = parser.parse_args()

    def resolve_config_path(input_path, base_dir):
        if os.path.isabs(input_path):
            return input_path
        return os.path.join(base_dir, 'config', input_path)

    args.env = resolve_config_path(args.env, main_path)
    args.master = resolve_config_path(args.master, main_path)
    args.list = resolve_config_path(args.list, main_path)

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")

    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else os.path.join(main_path, 'log')

    logger, log_path = setup_logging(final_log_dir, 'gp_export', global_date_folder, global_ts)
    
    logger.info("============================================================")
    logger.info("Started with concurrency: {0}".format(args.concurrency))
    logger.info("Resolved env config path: {0}".format(args.env))
    logger.info("Resolved master config path: {0}".format(args.master))
    logger.info("Resolved list file path: {0}".format(args.list))
    logger.info("============================================================")

    try:
        job = GreenplumExportJob(args, logger, log_path, global_date_folder, global_ts, main_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e))
        sys.exit(1)