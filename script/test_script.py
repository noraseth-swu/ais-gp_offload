#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import shutil
import tempfile
import logging
import sys
try:
    from unittest.mock import MagicMock, patch
except ImportError:
    from mock import MagicMock, patch

# Import คลาสจาก script หลัก
from script_query_greenplum import Config, QueryBuilder, FileHandler, ShellHandler, ProcessTracker, GreenplumExportJob

class TestGreenplumExport(unittest.TestCase):

    def setUp(self):
        """ทำงานก่อนเริ่ม Test"""
        # 1. ใช้โฟลเดอร์จริงชื่อ test_debug_output เพื่อเก็บไฟล์ผลลัพธ์ไว้ดู
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.test_dir = os.path.join(current_dir, 'test_debug_output')
        
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
            
        os.makedirs(self.test_dir)
            
        print("\n[DEBUG] Output files will be saved to: {0}".format(self.test_dir))

        # 2. กำหนด Path ไปที่ไฟล์ Config ของจริง
        config_dir = os.path.join(current_dir, 'config')
        self.master_config_path = os.path.join(config_dir, 'config_master.txt')
        self.list_file_path = os.path.join(config_dir, 'list_table.txt')

        # 3. สร้าง env config จำลอง ให้ชี้ local_temp_dir มาที่ test_debug_output
        self.env_config_path = os.path.join(self.test_dir, 'env_config_test.txt')
        
        # Setup Logger
        self.logger = logging.getLogger("TestLogger")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

        if not os.path.exists(self.master_config_path):
            raise FileNotFoundError("Could not find real file: {0}".format(self.master_config_path))

    def tearDown(self):
        # ไม่ลบโฟลเดอร์ เพื่อให้ user เข้าไปดูไฟล์ SQL ได้
        pass

    def create_test_env_config(self):
        """สร้างไฟล์ env config จำลอง"""
        with open(self.env_config_path, 'w') as f:
            # บังคับให้ Output ลงที่ folder test_debug_output
            f.write("local_temp_dir={0}\n".format(self.test_dir))
            f.write("nas_destination={0}\n".format(os.path.join(self.test_dir, 'nas')))

    # ==========================================================================
    # Test Case ใหม่: สั่งรัน Job จริงๆ ให้ครบทุก Table
    # ==========================================================================
    @patch('script_query_greenplum.ShellHandler.run_psql')
    @patch('script_query_greenplum.FileHandler.copy_to_nas')
    def test_full_job_execution_with_real_files(self, mock_nas, mock_psql):
        """
        ทดสอบการรัน Loop การทำงานจริง โดยอ่านจากไฟล์ list_table.txt ของจริง
        ผลลัพธ์: ต้องได้ไฟล์ .sql ตามจำนวน Table ใน list
        """
        self.create_test_env_config()
        
        print("\n--- Executing Full Job with Real Config ---")
        
        # 1. จำลองการทำงานของ PSQL และ NAS (เพื่อไม่ให้ Error เพราะเราไม่มี DB จริง)
        mock_psql.return_value = None # สมมติว่ารัน psql ผ่าน
        mock_nas.return_value = None  # สมมติว่า copy ผ่าน

        # 2. เริ่มต้น Job
        job = GreenplumExportJob(
            self.env_config_path, 
            self.master_config_path, 
            self.list_file_path, 
            cli_tables=None, 
            logger=self.logger
        )

        # 3. สั่งรัน (ตรงนี้แหละที่จะเกิดการ Loop สร้างไฟล์)
        job.run()

        # 4. ตรวจสอบผลลัพธ์ในโฟลเดอร์
        files = os.listdir(self.test_dir)
        sql_files = [f for f in files if f.endswith('.sql')]
        
        print("\n[RESULT] Generated {0} SQL files:".format(len(sql_files)))
        for f in sql_files:
            print(" - {0}".format(f))

        # Assert ว่าจำนวนไฟล์ SQL ต้องเท่ากับจำนวน Table ใน List ที่เราโหลดมา
        expected_count = len(job.config.execution_list)
        self.assertEqual(len(sql_files), expected_count, 
                         "Should generate SQL files equal to number of tables in list")

if __name__ == '__main__':
    unittest.main()