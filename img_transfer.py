import pymysql
import os, sys, time
import datetime

import threading

import cv2
import numpy as np
from PIL import Image
import io, base64

SRC_WIDTH = 1080
SRC_HEIGHT = 720

UPLOAD_INTERVAL = 10.0 # sec.

'''
MySQL Query example

SHOW COLUMNS FROM [TABLE_NAME];
SELECT [ATTRIBUTE_NAME_LIST] FROM [TABLE_NAME];
TRUNCATE TABLE [TABLE_NAME];
SELECT COUNT(*) FROM [TABLE_NAME];
'''


class Capture:
    def __init__(self):
        self.url = 'rtsp://ttng:1q2w3e4r@192.168.2.110:554/cam/realmonitor?channel=1&subtype=0'
        self.cap = None
        
        self.img_idx = 0
        self.robot_id = 91
        
        self.sql_conn = None
        self.sql_cur = None
        
        self.sql_user = 'parking'
        self.sql_host = '192.168.0.15'
        self.sql_password = 'client'
        self.sql_db = 'imgdb'
        self.sql_chst = 'utf8'
        
        self.operation = True
        
        self.frame = np.zeros((SRC_HEIGHT, SRC_WIDTH, 3), dtype=np.uint8)
        self.capture_flag = None
        
        self.lock = threading.Lock()
        
        self.capture_thread = threading.Thread(target=self.capture_task, args=())
        self.sql_thread = threading.Thread(target=self.sql_task, args=())
        
    
    def capture_task(self):
        self.cap = cv2.VideoCapture(self.url)
        
        op = True
        
        while self.cap.isOpened() and op:
            ret, frame = self.cap.read()
            
            self.lock.acquire()
            self.capture_flag = ret
            if ret:
                self.frame = frame.copy()
            op = self.operation
            self.lock.release()
            
            key = cv2.waitKey(10)
            
            if key == ord('='):
                self.lock.acquire()
                self.operation = False
                self.lock.release()
                break
            
    def sql_task(self):
        op = True
        
        self.sql_conn = pymysql.connect(
            host=self.sql_host,
            user=self.sql_user,
            password=self.sql_password,
            db=self.sql_db,
            charset=self.sql_chst
        )
        self.sql_cur = self.sql_conn.cursor(pymysql.cursors.DictCursor)
        
        while op:
            self.lock.acquire()
            op = self.operation
            flag = self.capture_flag
            frame = self.frame.copy()
            self.lock.release()
            
            if flag:
                image_buffer = Image.fromarray(frame)
                img_byte_array = io.BytesIO()
                image_buffer.save(img_byte_array, quality=70, format='PNG')
                img_str = base64.b64encode(img_byte_array.getvalue())
                img_str = img_str.decode('UTF-8')
                
                now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                # sql_insert_query = 'INSERT INTO imgs (idx, robot_id, image, width, height, datetime) VALUES ({}, {}, {}, {}, {}, {});'.format(
                #     self.img_idx, 
                #     self.robot_id, 
                #     img_str, 
                #     SRC_WIDTH, 
                #     SRC_HEIGHT, 
                #     now)
                sql_insert_query = 'INSERT INTO imgs (idx, robot_id, image, width, height, datetime) VALUES (%s, %s, %s, %s, %s, %s);'
                insert_blob_tuple = (self.img_idx, self.robot_id, img_str, SRC_WIDTH, SRC_HEIGHT, now)
                self.sql_cur.execute(sql_insert_query, insert_blob_tuple)
                self.sql_conn.commit()
                self.img_idx += 1
        
            time.sleep(UPLOAD_INTERVAL)
        
        self.sql_conn.close()
    
    def run(self):
        try:
            self.capture_thread.start()
            self.sql_thread.start()
        finally:
            if self.capture_thread.is_alive():
                self.capture_thread.join()
            if self.sql_thread.is_alive():
                self.sql_thread.join()
                

if __name__ == '__main__':
    cap = Capture()
    
    cap.run()
        
        
        
        