import os, sys, time
import pymysql
import cv2
import base64
import numpy as np
from PIL import Image
from io import BytesIO


class Reader:
    def __init__(self):
        self.sql_host = '192.168.0.15'
        self.sql_user = 'supervisor'
        self.sql_password = 'admin'
        self.sql_db = 'imgdb'
        self.sql_chst = 'utf8'
        
        self.img = None
        
        self.sql_conn = None
        self.sql_cur = None
        
        self.img_idx = None
        self.robot_id = None
        self.datetime = None
        self.src_width = None
        self.src_height = None
    
    def description(self):
        ret = self.img.copy()
        
        ret = cv2.rectangle(ret, (0, 0), (220, 75), (0, 0, 0), -1)
        
        ret = cv2.putText(ret, 'DATETIME : {}'.format(self.datetime), (5, 15), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255))
        ret = cv2.putText(ret, 'IMG SIZE : {} x {}'.format(self.src_width, self.src_height), (5, 35), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255))
        ret = cv2.putText(ret, 'ROBOT ID : {}'.format(self.robot_id), (5, 55), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255))
        
        return ret
    
    def run(self):
        self.sql_conn = pymysql.connect(
            host=self.sql_host,
            user=self.sql_user,
            password=self.sql_password,
            db=self.sql_db,
            charset=self.sql_chst
        )
        
        self.sql_cur = self.sql_conn.cursor(pymysql.cursors.DictCursor)
        
        sql_query = 'SELECT * FROM imgs;'
        self.sql_cur.execute(sql_query)
        records = self.sql_cur.fetchall()
        
        self.sql_cur.close()
        self.sql_conn.close()
        
        print(len(records))
        for r in records:
            # print(r.keys())
            self.img_idx = r['idx']
            self.robot_id = r['robot_id']
            
            self.src_width = r['width']
            self.src_height = r['height']
            self.datetime = r['datetime']
            
            img = base64.decodebytes(r['image'])
            img = Image.open(BytesIO(img))
            
            self.img = np.array(img, dtype=np.uint8)
            print(self.datetime)
            img = self.description()
            cv2.imshow('IMG', img)
            
            cv2.waitKey(0)
            
        cv2.destroyAllWindows()
        
        
if __name__ == '__main__':
    reader = Reader()
    
    reader.run()

        