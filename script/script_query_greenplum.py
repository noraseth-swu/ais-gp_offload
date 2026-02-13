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
from datetime import datetime

# ==============================================================================
# 1. Utilities: Logger & ProcessTracker
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, logger):
        self.logger = logger
        self.steps = []
        self.results = []
        self.start_time = None

    def start_step(self, step_name):
        self.start_time = time.time()
        self.logger.info(">>> Start Step: {0}".format(step_name))
        return step_name
    
    def end_step(self, step_name):
        if self.start_time:
            duration = time.time() - self.start_time
            self.steps.append({'step': step_name, 'duration': duration})
            self.logger.info("<<< End Step: {0} (Duration: {1:.2f}s)".format(step_name, duration))
            self.start_time = None

    def add_result(self, table_name, status, message="-"):
        """บันทึกสถานะการทำงานของแต่ละตาราง"""
        self.results.append({
            'table': table_name,
            'status': status,
            'message': str(message).replace('\n', ' ')
        })

    def print_summary(self, log_path):
        self.logger.info("="*80)
        self.logger.info("PROCESS STEP REPORT")
        self.logger.info("="*80)

        if not self.steps:
            self.logger.info("No steps recorded.")
        else:
            h_step = "Step Name"
            h_duration = "Duration"

            max_w_step = len(h_step)
            for s in self.steps:
                if len(s['step']) > max_w_step:
                    max_w_step = len(s['step'])
            
            w_step = max_w_step + 2
            w_duration = 15

            row_fmt = "{0:<{ws}} | {1:>{wd}}"

            header_line = row_fmt.format(h_step, h_duration, ws=w_step, wd=w_duration)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 50: sep_line = "-" * 50

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            total_time = 0
            for s in self.steps:
                duration_str = "{0:.2f}s".format(s['duration'])
                self.logger.info(row_fmt.format(s['step'], duration_str, ws=w_step, wd=w_duration))
                total_time += s['duration']
            
            self.logger.info(sep_line)
            self.logger.info("Total Execution Time: {0:.2f}s".format(total_time))
        
        self.logger.info("")

        self.logger.info("="*80)
        self.logger.info("TABLE EXECUTION SUMMARY")
        self.logger.info("="*80)

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
            self.logger.info("Total Tables: {0}".format(len(self.results)))
        
        self.logger.info("")
        self.logger.info("Log File: {0}".format(log_path))

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

    if logger.handlers:
        logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

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
        
        # 1. Load Environment Config (YAML)
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
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = "query_{0}_{1}_{2}.sql".format(db, table, ts)
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
        cmd = [
            'psql',
            '-A', '-t',
            '-c', sql,
            '-o', output_path
        ]

        if db_name:
            cmd.extend(['-d', db_name])

        self.logger.info("Executing PSQL... (DB: {0})".format(db_name or 'Default'))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                raise RuntimeError("PSQL execution failed. Return Code: {0}. Error: {1}".format(process.returncode, stderr))
            
            # Check if file exists and has content
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                self.logger.info("Query result saved to: {}".format(output_path))
            else:
                self.logger.warning("Query executed but output file is empty or missing: {0}".format(output_path))

        except OSError as e:
            self.logger.error("OS Error executing psql: {0}".format(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error in run_psql: {0}".format(e))
            raise

class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def copy_to_nas(self, src, dest_dir):
        try:
            self.logger.info("Copying {0} -> {1}".format(src, dest_dir))

            if not os.path.exists(src):
                raise IOError("Source file does not exist: {0}".format(src))
            
            if not os.path.exists(dest_dir):
                self.logger.info("Destination directory not found. Creating: {0}".format(dest_dir))
                os.makedirs(dest_dir)

            shutil.copy2(src, dest_dir)
            self.logger.info("Successfully copied file.")

        except shutil.Error as e:
            self.logger.error("Shutil error during copy: {0}".format(e))
        except IOError as e:
            self.logger.error("IO Error during: {0}".format(e))
            raise
        except Exception as e:
            self.logger.error("Unexpected error during copy: {0}".format(e))
            raise

# ==============================================================================
# 4. Orchestrator
# ==============================================================================

class GreenplumExportJob(object):
    def __init__(self, env_f, master_f, list_f, cli_tables, logger, log_path):
        self.logger = logger
        self.log_path = log_path
        self.tracker = ProcessTracker(logger)

        step = self.tracker.start_step("Initialization")
        try:
            self.config = Config(env_f, master_f, list_f, cli_tables, logger)
            self.builder = QueryBuilder(self.config.local_temp_dir, logger)
            self.shell = ShellHandler(logger)
            self.file_h = FileHandler(logger)
        except Exception as e:
            self.logger.critical("Initialization Failed: {0}".format(e))
            raise
        finally:
            self.tracker.end_step(step)

    def run(self):
        self.logger.info("Starting Batch Processing...")

        self.total_table = len(self.config.execution_list)
        for task in self.config.execution_list:
            db = task['db']
            schema = task['schema']
            table = task['table']

            full_table_name = "{0}.{1}.{2}".format(db, schema, table)
            job_name = "{0}.{1}".format(schema, table)
            step_main = self.tracker.start_step("Process Table: {0}".format(job_name))

            try:
                # 1. Lookup in Master Config
                key = (db, schema, table)
                master_info = self.config.master_data.get(key)

                if not master_info:
                    self.logger.warning("Skipping {0}: Not Found in Master Config.".format(job_name))
                    self.tracker.add_result(full_table_name, "SKIPPED", "Not Found in Master Config")
                    continue

                self.logger.info("Found config for {0} (Partition: {1})".format(job_name, master_info['partition']))

                # 2. Build SQL
                step_sql = self.tracker.start_step("Build SQL {0}".format(job_name))
                sql = self.builder.build_and_save_query(
                    db, schema, table,
                    master_info['partition'],
                    master_info['cols'],
                    master_info['where']
                )
                self.tracker.end_step(step_sql)

                # 3. Define Output Filename
                timestamp = datetime.now().strftime("%Y%m%d")
                output_filename = "{0}_{1}_{2}.txt".format(db, table, timestamp)
                local_path = os.path.join(self.config.local_temp_dir, output_filename)

                # 4. Execute PSQL
                step_exec = self.tracker.start_step("Execute Query {0}".format(job_name))
                self.shell.run_psql(sql, local_path, db)
                self.tracker.end_step(step_exec)

                # 5. Move to NAS
                step_move = self.tracker.start_step("Move to NAS {0}".format(job_name))
                self.file_h.copy_to_nas(local_path, self.config.nas_dest_base)
                self.tracker.end_step(step_move)

                self.logger.info("DONE: {0}".format(job_name))
                self.tracker.add_result(full_table_name, "SUCCESS", "-")

            except Exception as e:
                self.logger.error("FAILED: {0} | Reason: {1}".format(job_name, e))
                self.tracker.add_result(full_table_name, "FAILED", str(e))
                # Continue to next task, don't stop the whole batch

            finally:
                self.tracker.end_step(step_main)

        # Final Summary
        self.tracker.print_summary(self.log_path)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Greenplum Data Export Tool')
    parser.add_argument('--env', default='config.txt', help='Path to environment config text file')
    parser.add_argument('--master', default='config_master.txt', help='Path to master config pipe-delimited')
    parser.add_argument('--list', default='list_table.txt', help='Path to list of tables to run')
    parser.add_argument('--table_name', help='Optional: Specific tables to run (DB|Schema.Table) comma separated')

    args = parser.parse_args()

    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    default_log_dir = os.path.join(current_script_dir, 'logs')
    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else default_log_dir

    logger, log_path = setup_logging(final_log_dir, 'gp_export')

    logger.info("Arguments: {0}".format(args))    

    try:
        job = GreenplumExportJob(args.env, args.master, args.list, args.table_name, logger, log_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e))
        sys.exit(1)