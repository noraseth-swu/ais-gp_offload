#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import logging
import subprocess
import time
import argparse
import threading
import Queue
import glob
import re
from datetime import datetime

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

    def set_total_task(self, total):
        self.total_task = total

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, gp_count="-", sp_count="-", duration=0.0, remark="-"):
        with self.lock:
            self.results.append({
                'table': table_name, 
                'status': status,
                'gp_count': gp_count,
                'sp_count': sp_count,
                'duration': duration,
                'remark': str(remark).replace('\n', ' ')
            })
            self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
        
    def print_summary(self, log_path, output_path):
        self.logger.info("="*100)
        self.logger.info("RECONCILE EXECUTION SUMMARY")
        self.logger.info("="*100)

        success_count = 0
        warning_count = 0
        failed_count = 0
        skipped_count = 0

        if not self.results:
            self.logger.info("No Partitions processed.")
        else:
            h_table = "Partition Name"
            h_status = "Status"
            h_gpc = "GP Count"
            h_spc = "SP Count"
            h_dur = "Duration(s)"
            h_msg = "Remark"

            max_w_table = len(h_table)
            max_w_status = len(h_status)

            for r in self.results:
                if len(r['table']) > max_w_table: max_w_table = len(r['table'])
                if len(r['status']) > max_w_status: max_w_status = len(r['status'])

                if r['status'] == 'COMPLETED': success_count += 1
                elif r['status'] == 'WARNING': warning_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
                elif r['status'] == 'SKIPPED': skipped_count +=1
            
            w_table = max_w_table + 2
            w_status = max_w_status + 2

            row_fmt = "{0:<{wt}} | {1:<{ws}} | {2:<12} | {3:<12} | {4:<11} | {5}"
            header_line = row_fmt.format(h_table, h_status, h_gpc, h_spc, h_dur, h_msg, wt=w_table, ws=w_status)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 90: sep_line = "-" * 90

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            for r in self.results:
                dur_str = "{0:.2f}".format(r.get('duration', 0.0))
                self.logger.info(row_fmt.format(
                    r['table'], r['status'], str(r['gp_count']), str(r['sp_count']), 
                    dur_str, r['remark'], wt=w_table, ws=w_status
                ))
            
            self.logger.info(sep_line)
            self.logger.info("Total: {0} | Completed: {1} | Warning: {2} | Failed: {3} | Skipped: {4}".format(
                len(self.results), success_count, warning_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Write summary to output directory
        summary_file = os.path.join(output_path, "reconcile_summary_{0}.txt".format(datetime.now().strftime("%Y%m%d_%H%M%S")))
        try:
            with open(summary_file, 'w') as f:
                f.write("Total: {0}\nCompleted: {1}\nFailed: {2}\nSkipped: {3}\n".format(
                    len(self.results), success_count, failed_count, skipped_count))
        except Exception as e:
            self.logger.error("Could not write summary file to output: {0}".format(e))

        # Print Summary to Console
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total    : {0}".format(len(self.results)))
        print("Completed: {0}".format(success_count))
        print("Warning  : {0}".format(warning_count))
        print("Failed   : {0}".format(failed_count))
        print("Skipped  : {0}".format(skipped_count))
        print("Log File : {0}".format(log_path))
        print("Output   : {0}".format(summary_file))
        print("="*80)

def setup_logging(log_dir, log_name="app", timestamp=None):
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Error: {1}".format(log_dir, e))
            log_dir = '.'

    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))
    logger = logging.getLogger("ReconcileBatch")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger, log_file

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class ConfigManager(object):
    def __init__(self, env_config_path, list_file_path, cli_tables, logger):
        self.logger = logger

        # 1. Load Environment Config
        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.succeed_path = ''
        self.hdfs_path = ''
        self.greenplum_result_path = ''
        self.replace_path_from = ''
        self.replace_path_to = ''
        self.datatype_mapping_path = ''

        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if key == 'succeed_path': self.succeed_path = value
                        elif key =='hdfs_path': self.hdfs_path = value
                        elif key =='greenplum_result_path': self.greenplum_result_path = value
                        elif key == 'replace_path_from': self.replace_path_from = value
                        elif key == 'replace_path_to': self.replace_path_to = value
                        elif key == 'datatype_mapping_path': self.datatype_mapping_path = value
        except IOError as e:
            self.logger.critical("Cannot find env_config file: {0}".format(e))
            raise

        # 2. Determine Execution List
        self.execution_list = []
        if cli_tables:
            self.logger.info("Using CLI arguments for table list.")
            tables = cli_tables.split(',')
            for t in tables:
                try:
                    db_part, tbl_part = t.split('|')
                    self.execution_list.append({'db': db_part.strip(), 'partition': tbl_part.strip()})
                except ValueError:
                    self.logger.error("Invalid format in argument: {0}. Expected DB|Schema.Partition".format(t))
        else:
            self.logger.info("Using list file: {0}".format(list_file_path))
            try:
                with open(list_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): continue
                        try:
                            db_part, tbl_part = line.split('|')
                            sch_part, real_tbl = tbl_part.split('.')
                            self.execution_list.append({
                                'db': db_part.strip(), 
                                'schema': sch_part.strip(), 
                                'partition': real_tbl.strip()
                            })
                        except ValueError:
                            self.logger.warning("Skipping invalid line in list file: {0}".format(line))
            except IOError as e:
                self.logger.critical("Cannot find list_table file: {0}".format(e))
                raise

# ==============================================================================
# 3. Handler & Helper Classes
# ==============================================================================

class LogParser(object):
    def __init__(self, succeed_base_path, logger):
        self.succeed_base_path = succeed_base_path
        self.logger = logger
        self.cache = {} 
        self.lock = threading.Lock()

    def get_latest_succeed_info(self, db, schema, partition):
        cache_key = "{0}_{1}".format(db, schema)
        
        with self.lock:
            if cache_key not in self.cache:
                self.logger.info("[LogParser] Building memory cache for DB: {0}, Schema: {1} ...".format(db, schema))
                self.cache[cache_key] = {}
                
                search_pattern = os.path.join(self.succeed_base_path, db, "*", "offloadgp_stat_succeeded.{0}.csv".format(schema))
                # search_pattern = os.path.join(self.succeed_base_path, db, "backup_old_file", "*", "offloadgp_stat_succeeded.{0}.csv".format(schema))
                self.logger.info("[LogParser] Searching for succeed log using pattern: {0}".format(search_pattern))
                matched_files = sorted(glob.glob(search_pattern), reverse=True)

                if not matched_files:
                    self.logger.warning("[LogParser] Log files not found for pattern: {0}".format(search_pattern))
                else:
                    for log_file in matched_files:
                        self.logger.info("[LogParser] Scanning log file into cache: {0}".format(log_file))
                        try:
                            with open(log_file, 'r') as f:
                                reader = csv.DictReader(f)
                                for row in reader:
                                    gp_tbl = row.get('Greenplum_Tbl', '')
                                    status = row.get('Run_Status', '')
                                    
                                    if gp_tbl and status == 'SUCCEEDED':
                                        if gp_tbl not in self.cache[cache_key]:
                                            self.cache[cache_key][gp_tbl] = row
                                        else:
                                            current_ts = self.cache[cache_key][gp_tbl].get('End_Timestamp_Script', '')
                                            new_ts = row.get('End_Timestamp_Script', '')
                                            if new_ts > current_ts:
                                                self.cache[cache_key][gp_tbl] = row
                        except Exception as e:
                            self.logger.warning("[LogParser] Error parsing log file {0}: {1}".format(log_file, e))
                self.logger.info("[LogParser] Cache build completed. Total {0} tables cached.".format(len(self.cache[cache_key])))

        target_table_with_schema = "{0}.{1}".format(schema, partition)
        
        latest_row = self.cache[cache_key].get(partition) or self.cache[cache_key].get(target_table_with_schema)
        
        if latest_row:
            self.logger.info("[LogParser] Found latest SUCCEEDED record for {0} from Cache. Target Parquet: {1}".format(
                partition, latest_row.get('File_Path', 'N/A')))
            return latest_row, "Found SUCCEEDED record"
        else:
            self.logger.warning("[LogParser] Status is not SUCCEEDED in cache for {0}".format(partition))
            return None, "Status is not SUCCEEDED in any log files"
    
class HDFSHandler(object):
    def __init__(self, spark_session, logger):
        self.logger = logger
        self.spark = spark_session

        self.sc = self.spark.sparkContext
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)
        self.Path = self.sc._jvm.org.apache.hadoop.fs.Path

    def sync_parquet(self, local_file_path, hdfs_dest_path):
        self.logger.info("[HDFSHandler] Evaluating HDFS sync. Local: {0} -> HDFS: {1}".format(local_file_path, hdfs_dest_path))
        try:
            local_mtime = os.path.getmtime(local_file_path)
        except OSError as e:
            raise RuntimeError("Local path not found or inaccessible: {0}".format(e))
        
        local_path_str = "file:///" + os.path.abspath(local_file_path).replace("\\", "/").lstrip("/")
        local_path_obj = self.Path(local_path_str)
        hdfs_path_obj = self.Path(hdfs_dest_path)

        needs_upload = False

        if not self.fs.exists(hdfs_path_obj):
            needs_upload = True
        else:
            try:
                file_status = self.fs.getFileStatus(hdfs_path_obj)
                hdfs_mtime = file_status.getModificationTime() / 1000.0

                if local_mtime > hdfs_mtime:
                    needs_upload = True
            except Exception as e:
                self.logger.warning("Could not get HDFS file status via JVM. Forcing upload. Error: {0}".format(e))
                needs_upload = True

        if needs_upload:
            self.logger.info("[HDFSHandler] Proceeding to upload Parquet to HDFS: {0}".format(hdfs_dest_path))
            try:
                parent_path = hdfs_path_obj.getParent()
                if parent_path and not self.fs.exists(parent_path):
                    self.fs.mkdirs(parent_path)
                
                self.fs.copyFromLocalFile(False, True, local_path_obj, hdfs_path_obj)
            except Exception as e:
                raise RuntimeError("HDFS copyFromLocalFile via JVM failed: {0}".format(e))
        else:
            self.logger.info("[HDFSHandler] Parquet file is up-to-date. Skipping HDFS upload.")
        return True

class GPResultParser(object):
    def __init__(self, result_base_path, logger):
        self.result_base_path = result_base_path
        self.logger = logger

    def parse_result(self, db, partition):
        table_name_only = partition.split('.')[-1] 
        search_pattern = os.path.join(self.result_base_path, "*", "{0}_{1}_*.txt".format(db, table_name_only))
        
        self.logger.info("[GPResultParser] Searching for GP result using pattern: {0}".format(search_pattern))

        matched_files = glob.glob(search_pattern)

        if not matched_files:
            self.logger.warning("[GPResultParser] GP result file not found for pattern: {0}".format(search_pattern))
            raise IOError("Greenplum result file not found for pattern: {0}".format(search_pattern))
        
        latest_file = max(matched_files, key=os.path.getmtime)
        self.logger.info("[GPResultParser] Found GP result file. Reading from: {0}".format(latest_file))
        try:
            with open(latest_file, 'r') as f:
                data = f.read().strip()

            parts = data.split('|')
            if len(parts) < 4:
                raise ValueError("Invalid GP result format: {0}".format(data))
            
            record_count = int(parts[3])
            metrics = {}
            status_remark = ""

            if len(parts) > 4 and parts[4].strip():
                metric_str = parts[4].strip()
                self.logger.info("[Debug] Raw GP Metric String: '{0}'".format(metric_str))
                
                if "MIN_MAX_STATUS:" in metric_str:
                    status_split = metric_str.split("MIN_MAX_STATUS:")
                    metric_str = status_split[0].strip().rstrip(',')
                    status_remark = "MIN_MAX_STATUS: " + status_split[1].strip()

                if metric_str:
                    pairs = metric_str.split(',')
                    for pair in pairs:
                        if ':' in pair:
                            func_col, val = pair.split(':', 1)
                            if '(' in func_col and ')' in func_col:
                                func = func_col.split('(')[0].strip().upper()
                                col = func_col.split('(')[1].split(')')[0].strip()
                                if col not in metrics:
                                    metrics[col] = {}
                                metrics[col][func] = val.strip()

            self.logger.info("[Debug] Parsed Metrics: {0} | Remark: '{1}'".format(metrics, status_remark))

            return record_count, metrics, status_remark
        except Exception as e:
            self.logger.exception("Error parsing GP result: {0}".format(latest_file))
            raise RuntimeError("Failed to parse GP Result: {0}".format(e))
        
class DataTypeParser(object):
    def __init__(self, base_path, logger):
        self.base_path = base_path
        self.logger = logger

    def get_mapping(self, partition):
        mapping = {}
        if not self.base_path:
            self.logger.warning("[DataTypeParser] Base path is Empty! Skipping data type mapping.")
            return mapping
        
        self.logger.info("[DataTypeParser] Searching for Data Type mapping file in: {0} (keyword: {1})".format(self.base_path, partition))
        
        for root, dirs, files in os.walk(self.base_path):
            for file_name in files:
                if file_name.endswith('.csv') and partition in file_name:
                    targer_file = os.path.join(root, file_name)
                    try:
                        with open(targer_file, 'r') as f:
                            reader = csv.DictReader(f, delimiter='|')
                            for row in reader:
                                ext_col = row.get('ext_column_nm', '').strip()
                                gp_type = row.get('gp_datatype', '').strip()
                                if ext_col and gp_type:
                                    mapping[ext_col] = gp_type.lower()
                        self.logger.info("[DataTypeParser] Loaded mapping from {0}".format(targer_file))
                        return mapping
                    except Exception as e:
                        self.logger.warning("[DataTypeParser] Error reading mapping file {0}: {1}".format(targer_file, e))
        self.logger.warning("[DataTypeParser] Mapping file not found for: {0}".format(partition))
        return mapping
    
    def get_spark_type(self, gp_type):
        gp_type = gp_type.lower().strip()
        match = re.search(r'\(.*?\)', gp_type)
        length_str = match.group(0) if match else ""

        if 'num' in gp_type or 'dec' in gp_type:
            if length_str:
                return 'decimal' + length_str
            return 'decimal(38,4)'
        if 'char' in gp_type or 'text' in gp_type:
            return 'string'
        if 'int' in gp_type: 
            return 'long'
        if 'date' in gp_type: 
            return 'date'
        if 'time' in gp_type: 
            return 'timestamp'
        return 'string'

class HiveLogger(object):
    def __init__(self, spark_session, logger):
        self.spark = spark_session
        self.logger = logger
        self.target_table = "output_reconcile_sum_columns"
        self.insert_lock = threading.Lock()

    def log_result(self, execution_id, db, schema, table, partition, start_ts, end_ts, 
                   duration_exec, duration_table, 
                   gp_count, sp_count, status, agg_type, col_name, val_gp, val_sp, remark):
        safe_val_gp = str(val_gp).replace("'", "") if val_gp is not None else ""
        safe_val_sp = str(val_sp).replace("'", "") if val_sp is not None else ""
        safe_remark = str(remark).replace("'", "") if remark is not None else ""
        insert_sql = """
            INSERT INTO TABLE {0}
            VALUES (
                '{1}', '{2}', '{3}', '{4}', '{5}',
                cast('{6}' as timestamp), cast('{7}' as timestamp),
                cast({8} as decimal(18,2)), cast({9} as decimal(18,2)),
                {10}, {11}, '{12}',
                '{13}', '{14}', '{15}', '{16}',
                '{17}')
        """.format(
            self.target_table,
            execution_id, db, schema, table, partition,
            start_ts.strftime('%Y-%m-%d %H:%M:%S'), end_ts.strftime('%Y-%m-%d %H:%M:%S'),
            duration_exec, duration_table,
            gp_count, sp_count, status,
            agg_type, col_name, safe_val_gp, safe_val_sp, safe_remark
        )
        try:
            with self.insert_lock:
                self.spark.sql(insert_sql)
        except Exception as e:
            self.logger.exception("Hive Insert Failed for {0}".format(partition))

# ==============================================================================
# 4. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, log_parser, datatype_parser, hdfs_h, gp_parser, hive_logger, spark, tracker, logger, run_date, execution_id):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.log_parser = log_parser
        self.datatype_parser = datatype_parser
        self.hdfs_h = hdfs_h
        self.gp_parser = gp_parser
        self.hive_logger = hive_logger
        self.spark = spark
        self.tracker = tracker
        self.logger = logger
        self.run_date = run_date
        self.execution_id = execution_id
        self.daemon = True

    def run(self):
        while(True):
            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            db = task['db']
            base_schema = task['schema']
            partition = task['partition']
            start_datetime = datetime.now()
            start_t = time.time()

            base_table = partition.split('_1_prt_')[0] if '_1_prt_' in partition else partition

            try:
                self.tracker.update_worker_status(self.name, "[BUSY] {0}".format(partition))                
                
                # Step 1: Check Succeed Log
                log_row, log_msg = self.log_parser.get_latest_succeed_info(db, base_schema, partition)
                if not log_row:
                    raise ValueError("SKIPPED: " + log_msg)
                
                local_file_path = log_row.get('File_Path')
                if self.config.replace_path_from and self.config.replace_path_to:
                    original_path = local_file_path
                    local_file_path = local_file_path.replace(self.config.replace_path_from, self.config.replace_path_to)
                    
                    if original_path != local_file_path:
                        self.logger.info("[Worker] Replaced local_file_path from '{0}' to '{1}'".format(original_path, local_file_path))

                # Step 2: HDFS Sync
                hdfs_dest = os.path.join(self.config.hdfs_path, db, base_schema, partition)
                self.hdfs_h.sync_parquet(local_file_path, hdfs_dest)

                # Step 3: Parse GP Results & DataType Mapping
                gp_count, metrics, status_remark = self.gp_parser.parse_result(db, partition)
                datatype_map = self.datatype_parser.get_mapping(partition)
                
                # Step 4: Dynamic PySpark Aggregation
                sp_start_t = time.time()
                sp_count = 0
                sp_res = None
                log_buffer = []
                try:
                    self.spark.sparkContext.setJobGroup(partition, "Reconciling: " + partition)
                    df = self.spark.read.parquet(hdfs_dest)
                    agg_exprs = [F.count("*").alias("sp_count")]
                    
                    if metrics and isinstance(metrics, dict):
                        log_buffer.append("-" * 100)
                        log_buffer.append("[Investigate] Data Type Casting Summary | Table: {0}".format(partition))
                        log_buffer.append("-" * 100)
                        log_buffer.append("{0:<30} | {1:<35} | {2:<15}".format("Metric", "Greenplum Type", "Spark Type"))
                        log_buffer.append("-" * 100)
                        
                        for col, funcs in metrics.items():
                            if not isinstance(funcs, dict):
                                self.logger.warning("[Worker] Invalid metric format for col {0}: {1}".format(col, funcs))
                                continue
                                
                            gp_type = datatype_map.get(col.lower(), 'string')
                            spark_type = self.datatype_parser.get_spark_type(gp_type)
                            
                            for func, gp_val in funcs.items():
                                alias_name = "{0}_{1}".format(func, col)
                                col_expr = F.col(col).cast(spark_type)
                                
                                metric_name = "{0}({1})".format(func, col)
                                log_buffer.append("{0:<30} | {1:<35} | {2:<15}".format(metric_name, gp_type, spark_type))
                                
                                if func == 'SUM':
                                    agg_exprs.append(F.sum(col_expr).alias(alias_name))
                                elif func == 'MIN':
                                    agg_exprs.append(F.min(col_expr).alias(alias_name))
                                elif func == 'MAX':
                                    agg_exprs.append(F.max(col_expr).alias(alias_name))

                        log_buffer.append("-" * 100)

                    sp_res = df.agg(*agg_exprs).collect()[0]
                    sp_count = sp_res["sp_count"]
                except Exception as e:
                    raise RuntimeError("Spark Parquet Aggregation Failed: {0}".format(e))
                finally:
                    self.spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
                
                # Step 5: Smart Comparison & Logging
                end_datetime = datetime.now()
                duration_table = time.time() - start_t
                duration_exec = time.time() - self.tracker.start_time
                
                is_count_matched = (gp_count == sp_count)
                is_completed = is_count_matched
                mismatch_col_count = 0
                total_metrics = 0

                if metrics and isinstance(metrics, dict):
                    log_buffer.append("-" * 110)
                    log_buffer.append("[Investigate] Data Comparison Summary | Table: {0}".format(partition))
                    log_buffer.append("-" * 110)
                    log_buffer.append("{0:<30} | {1:<25} | {2:<25} | {3:<10}".format("Metric", "Greenplum Value", "Spark Value", "Matched"))
                    log_buffer.append("-" * 110)

                    for col, funcs in metrics.items():
                        if not isinstance(funcs, dict): continue
                        
                        gp_type = datatype_map.get(col.lower(), 'string')
                        for func, gp_val in funcs.items():
                            total_metrics += 1
                            alias_name = "{0}_{1}".format(func, col)
                            
                            sp_res_dict = sp_res.asDict()
                            sp_val = sp_res_dict.get(alias_name)
                            
                            is_col_matched = False
                            val_gp_str = str(gp_val).strip() if gp_val else ""
                            
                            if sp_val is None:
                                val_sp_str = ""
                            else:
                                val_type = type(sp_val).__name__
                                if val_type == 'datetime' or val_type == 'Timestamp':
                                    val_sp_str = sp_val.strftime('%Y-%m-%d %H:%M:%S')
                                elif val_type == 'date':
                                    val_sp_str = sp_val.strftime('%Y-%m-%d')
                                else:
                                    val_sp_str = str(sp_val).strip()

                            is_numeric = ('num' in gp_type or 'dec' in gp_type or 'int' in gp_type or func == 'SUM')
                            
                            if is_numeric and val_gp_str and val_sp_str:
                                try:
                                    g_f = float(val_gp_str)
                                    s_f = float(val_sp_str)
                                    is_col_matched = abs(g_f - s_f) < 0.001
                                    scale = 4
                                    
                                    if ('num' in gp_type or 'dec' in gp_type) and ',' in gp_type:
                                        try:
                                            scale = int(gp_type.split(',')[1].replace(')', '').strip())
                                        except:
                                            pass
                                    elif 'int' in gp_type:
                                        scale = 0
                                        
                                    fmt_str = "{0:." + str(scale) + "f}"
                                    val_gp_str = fmt_str.format(g_f)
                                    val_sp_str = fmt_str.format(s_f)

                                except ValueError:
                                    is_col_matched = (val_gp_str == val_sp_str)
                            else:
                                is_col_matched = (val_gp_str == val_sp_str)
                            
                            metric_name = "{0}({1})".format(func, col)
                            match_str = "True" if is_col_matched else "False"
                            log_buffer.append("{0:<30} | {1:<25} | {2:<25} | {3:<10}".format(metric_name, val_gp_str, val_sp_str, match_str))

                            if not is_col_matched:
                                mismatch_col_count += 1
                                is_completed = False

                            col_status = 'completed' if (is_count_matched and is_col_matched) else 'failed'
                            self.hive_logger.log_result(
                                self.execution_id, db, base_schema, base_table, partition,
                                start_datetime, end_datetime, duration_exec, duration_table,
                                gp_count, sp_count, col_status, func, col, val_gp_str, val_sp_str, "Matched" if is_col_matched else "Mismatch"
                            )
                    
                    log_buffer.append("-" * 110)
                else:
                    final_status = 'completed' if is_completed else 'failed'
                    self.hive_logger.log_result(
                        self.execution_id, db, base_schema, base_table, partition,
                        start_datetime, end_datetime, duration_exec, duration_table,
                        gp_count, sp_count, final_status, "COUNT_ONLY", "N/A", str(gp_count), str(sp_count), "Matched" if is_completed else "Count Mismatch"
                    )

                if log_buffer:
                    with self.tracker.lock:
                        for line in log_buffer:
                            self.logger.info(line)
                
                final_status = "COMPLETED" if is_completed else "FAILED"
                
                if final_status == "COMPLETED" and ("MISSING" in status_remark or not datatype_map):
                    self.logger.warning("[Debug] Status changed to WARNING. Reason -> GP_Remark: '{0}', Has_DataType_Map: {1}".format(status_remark, bool(datatype_map)))
                    final_status = "WARNING"
                
                if final_status in ["COMPLETED", "WARNING"]:
                    remark_msg = "Success"
                elif not is_count_matched:
                    remark_msg = "Count Mismatch"
                else:
                    remark_msg = "Data Mismatch ({0}/{1})".format(mismatch_col_count, total_metrics)
                
                if status_remark:
                    remark_msg += " | " + status_remark

                self.tracker.add_result(partition, final_status, gp_count, sp_count, duration_table, remark_msg)


            except ValueError as ve:
                duration_table = time.time() - start_t
                duration_exec = time.time() - self.tracker.start_time
                
                msg = str(ve)
                status = "SKIPPED" if "SKIPPED" in msg else "FAILED"
                self.tracker.add_result(partition, status, remark=msg)
                
                self.hive_logger.log_result(
                    self.execution_id, db, base_schema, base_table, partition,
                    start_datetime, datetime.now(), duration_exec, duration_table, 
                    0, 0, status.lower(), "N/A", 0, 0, msg
                )
            except Exception as e:
                duration_table = time.time() - start_t
                duration_exec = time.time() - self.tracker.start_time
                
                safe_err = repr(e)
                err_msg = str(e)
                
                if "No such file or directory" in err_msg or "Local path not found" in err_msg:
                    clear_remark = "Source Parquet Not Found (Check Mount/Path)"
                elif "Spark Parquet Aggregation Failed" in err_msg:
                    clear_remark = "PySpark Aggregation Failed (Check Data/Schema)"
                elif "Greenplum result file not found" in err_msg:
                    clear_remark = "GP Result File Missing"
                elif "Failed to parse GP Result" in err_msg:
                    clear_remark = "Invalid GP Result Format"
                else:
                    clear_remark = "Error: {0}".format(err_msg[:40] + "..." if len(err_msg) > 40 else err_msg)

                self.logger.warning("Worker {0} failed on {1}: {2}".format(self.name, partition, safe_err))
                
                self.tracker.add_result(partition, "FAILED", remark=clear_remark)
                
                self.hive_logger.log_result(
                    self.execution_id, db, base_schema, base_table, partition,
                    start_datetime, datetime.now(), duration_exec, duration_table, 
                    0, 0, "failed", "N/A", "0", "0", safe_err
                )
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

        lines = []
        lines.append("============================================================")
        lines.append(" RECONCILE PARQUET MONITOR (Python 2.7 / PySpark) ")
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
# 5. Main Job Class
# ==============================================================================

class ReconcileJob(object):
    def __init__(self, args, logger, log_path, out_path, global_date_folder, main_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.out_path = out_path
        self.global_date_folder = global_date_folder
        self.tracker = ProcessTracker(logger)

        self.config = ConfigManager(args.env, args.list, args.table_name, logger)

        self.logger.info("Initializing SparkSession with FAIR scheduler...")
        self.spark = SparkSession.builder \
            .appName("GP_Parquet_Reconcile") \
            .config("spark.scheduler.mode", "FAIR") \
            .enableHiveSupport() \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        self.execution_id = self.spark.sparkContext.applicationId
        self.logger.info("Assigned Execution ID (YARN App ID): {0}".format(self.execution_id))
        
        self.log_parser = LogParser(self.config.succeed_path, logger)
        self.hdfs_h = HDFSHandler(self.spark, logger)
        self.gp_parser = GPResultParser(self.config.greenplum_result_path, logger)
        self.hive_logger = HiveLogger(self.spark, logger)
        self.datatype_parser = DataTypeParser(self.config.datatype_mapping_path, logger)

        self.job_queue = Queue.Queue()
        for task in self.config.execution_list:
            self.job_queue.put(task)

        self.tracker.set_total_task(len(self.config.execution_list))
        self.logger.info("Loaded {0} tasks into queue.".format(len(self.config.execution_list)))

    def run(self):
        num_workers = int(self.args.concurrency)
        self.logger.info("Starting {0} workers...".format(num_workers))

        workers = []
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.log_parser, self.datatype_parser, 
                       self.hdfs_h, self.gp_parser, self.hive_logger, self.spark, 
                       self.tracker, self.logger, self.global_date_folder, self.execution_id)
            workers.append(w)
            w.start()

        monitor = MonitorThread(self.tracker, num_workers, self.log_path)
        monitor.start()

        try:
            self.job_queue.join()
            for w in workers:
                w.join()
        except KeyboardInterrupt:
            sys.stdout.write("\n\n>>> KEYBOARD INTERRUPT DETECTED! ABORTING SCRIPT... <<<\n\n")
            sys.stdout.flush()
            self.logger.warning("Keyboard Interrupt! User aborted the script.")
        finally:
            monitor.stop()
            monitor.join()
            self.spark.stop()
            self.tracker.print_summary(self.log_path, self.out_path)

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Greenplum to Parquet Reconcile Tool')
    parser.add_argument('--env', default='env_config.txt', help='Name of env config file')
    parser.add_argument('--list', default='list_table.txt', help='Name of list of tables file')
    parser.add_argument('--table_name', help='Optional: Specific tables to run (DB|Schema.Partition)')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel worker (Default: 4)')

    args = parser.parse_args()

    args.env = os.path.join(main_path, 'config', os.path.basename(args.env))
    args.list = os.path.join(main_path, 'config', os.path.basename(args.list))

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")

    final_log_dir = os.path.join(main_path, 'log', global_date_folder)
    final_out_dir = os.path.join(main_path, 'output', global_date_folder)

    if not os.path.exists(final_out_dir):
        try:
            os.makedirs(final_out_dir)
        except OSError as e:
            print("WARNING: Could not create output directory '{0}'. Using current directory. Error: {1}".format(final_out_dir, e))
    
    logger, log_path = setup_logging(final_log_dir, 'reconcile_job', global_ts)
    logger.info("Started Reconcile script with concurrency: {0}".format(args.concurrency))

    try:
        job = ReconcileJob(args, logger, log_path, final_out_dir, global_date_folder, main_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e), exc_info=True)
        sys.exit(1)