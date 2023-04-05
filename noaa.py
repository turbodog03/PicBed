# -*- coding: utf-8 -*-
# @Time        : 2021/4/7 18:56
# @Author      : DouHua
# @Email       : feng@dongfa.pro
# @File        : noaa.py
# @Project     : noaa
# @Description : 获取并处理NOAA数据集的主程序

import os
import sqlite3
from urllib.parse import urljoin

from lxml import etree
import pandas as pd
import requests

from config import NOAA_ARCHIVE_URL, DOWNLOAD_FOLDER, DB_NAME, BD_AK, NOAA_STATION_API
from toolbox import extract_tar, BaiduMap


class NOAA:
    """
    下载、处理NOAA开放数据
    """

    def __init__(self):
        # 设置原始数据下载目录
        self.folder = DOWNLOAD_FOLDER

        # NOAA官方网站的地址
        self.noaa_archive_url = NOAA_ARCHIVE_URL

        # NOAA获取站点信息的API地址
        self.noaa_station_api = NOAA_STATION_API

        # 创建Session管理访问会话
        self.s = requests.Session()

        # 数据库相关配置
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        # 创建新表，用来存储气象站的基本信息
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS info 
                   (
                       station_id CHAR(12) NOT NULL,
                       name       VARCHAR(50),
                       latitude   FLOAT,
                       longitude  FLOAT,
                       country    VARCHAR(20),
                       province   VARCHAR(20),
                       city       VARCHAR(20),
                       district   VARCHAR(20),
                       PRIMARY KEY (station_id)
                   );""")
        # 创建新表，用来存储所有的气象站数据
        # 由于气象站id和它的日期被设置成了主键，所以数据库当中肯定不会出现数据重复
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS data
                   (
                       station    CHAR(12) NOT NULL,
                       date       DATE NOT NULL,
                       temp       FLOAT,
                       dewp       FLOAT,
                       slp        FLOAT,
                       stp        FLOAT,
                       visib      FLOAT,
                       wdsp       FLOAT,
                       mxspd      FLOAT,
                       gust       FLOAT,
                       max        FLOAT,
                       min        FLOAT,
                       prcp       FLOAT,
                       sndp       FLOAT,
                       frshtt     int,
                       PRIMARY KEY (station, date)
                   );""")

        self.baidu_api = None

    def _download_tar_gz(self, url) -> None:
        """
        下载单个tar.gz压缩包
        :param url: tar.gz压缩包的下载链接
        :return: 直接将tar.gz文件存盘，无需返回
        """
        # 如果目标文件夹不存在，则需要创建该文件夹
        if not os.path.exists(self.folder) or os.path.isfile(self.folder):
            os.mkdir(self.folder)

        # 根据链接设置文件存储名称
        file_name = url.split('/')[-1]
        target_file_path = os.path.join(self.folder, file_name)

        # 开始下载压缩包文件
        resp = self.s.get(url, stream=True)
        if resp.status_code == 200:
            with open(target_file_path, 'wb') as f:
                for chunk in resp.raw.stream(1024, decode_content=False):
                    if chunk:
                        f.write(chunk)
                print(f'成功下载到压缩文件 {file_name} !')
        else:
            print(f'尝试访问 {url} 时出错！')

    def fetch_raw_data(self) -> None:
        """
        从NOAA网站上下载所有全球气象站的所有历史数据
        :return: 所下载文件直接存盘，无需返回值
        """
        # 首先拿到NOAA网页上所有的文件下载链接
        resp = self.s.get(self.noaa_archive_url).text
        html = etree.HTML(resp)
        file_url_list = html.xpath('//a[contains(@href, "tar.gz")]/@href')

        for url in file_url_list:
            # 如果某个文件已经下载了，就不再重复下载了
            if os.path.exists(os.path.join(self.folder, url)):
                print(f'{url} 已经下载了，不再重复下载！')
                continue

            # 如果文件不存在，那就需要拼接出真正的下载链接并执行下载任务
            self._download_tar_gz(urljoin(self.noaa_archive_url, url))

    def insert_raw_data(self) -> None:
        """
        解压、归并并持久化储存原始数据
        :return: 直接持久化储存到硬盘，无需返回值
        """
        # 解压压缩包文件
        for item in os.listdir(self.folder):
            tar_file_path = os.path.join(self.folder, item)
            if os.path.isfile(tar_file_path) and item.split('.')[-1] == 'gz':
                extract_tar(tar_file_path)

        # 遍历解压出来的csv文件，把它们都放进数据库
        for root, _, file in os.walk(self.folder):
            for f in file:
                if f.split('.')[-1] == 'csv':
                    current_path = os.path.join(root, f)
                    self._insert_single_csv(current_path)

    def _insert_single_csv(self, csv_path: str) -> None:
        """
        将单个csv文件当中的数据插入到数据库当中
        :param csv_path: csv文件的路径
        :return:
        """
        print(f'开始尝试将 {csv_path} 当中的所有数据插入到数据库当中......')

        # 将数据读入内存
        try:  # 有的csv文件存在一些问题，导致无法正常读取
            df = pd.read_csv(csv_path, usecols=['STATION', 'DATE', 'TEMP', 'DEWP', 'SLP', 'STP', 'VISIB', 'WDSP',
                                                'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT'])
        except:
            print(f'处理文件 {csv_path} 时出错，跳过该任务！')
            return

        # 拼接SQL语句，准备插入数据库
        sql_statement = "INSERT OR IGNORE INTO data(STATION, DATE, TEMP, DEWP, SLP, STP, VISIB, WDSP, " \
                        "MXSPD, GUST, MAX, MIN, PRCP, SNDP, FRSHTT) VALUES"
        value = ''
        for item in df.loc[:, ['STATION', 'DATE', 'TEMP', 'DEWP', 'SLP',
                               'STP', 'VISIB', 'WDSP', 'MXSPD', 'GUST',
                               'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT']].itertuples(index=False):
            item = [f'"{item[index]}"' for index in range(len(item))]
            value += f"({', '.join(item)}),"
        sql_statement += value[:-1] + ';'

        # 执行插入操作，向数据库中插入数据
        self.cursor.execute(sql_statement)
        self.conn.commit()

    def insert_station_info(self) -> None:
        """
        使用百度地图API插入站点的地理位置信息
        :return:
        """
        # 配置百度地图API
        self.baidu_api = BaiduMap(BD_AK)

        # 获取数据库当中所有的站点id
        ids = self.cursor.execute('SELECT DISTINCT data.station FROM data LEFT OUTER JOIN info ON data.station = '
                                  'info.station_id WHERE info.station_id IS NULL ORDER BY data.station DESC;')
        for station in ids:
            self._insert_single_station(station[0])

    def _insert_single_station(self, station_id: str) -> None:
        """
        插入单个站点的地理位置信息
        :param station_id: 气象站的id
        :return:
        """
        print(f'正在尝试联网获取站点 {station_id} 的经纬度信息...')
        params = {
            'stations': station_id,
            'dataset': 'global-summary-of-the-day',
            'limit': '1',
        }
        try:
            resp = self.s.get(url=self.noaa_station_api, params=params).json()
            info = {
                'station_id': station_id,
                'name': resp['results'][0]['stations'][0]['name'],
                'latitude': resp['bounds']['bottomRight']['lat'],
                'longitude': resp['bounds']['bottomRight']['lon'],
            }
        except:
            print(f'获取站点 {station_id} 相关信息时出错！')
            return

        location = self.baidu_api.get_location(info['latitude'], info['longitude'])
        info.update(location)

        # 检索得到信息后，尝试插入到数据库当中
        values = [f'"{v}"' for v in info.values()]
        sql_statement = f'INSERT INTO info({", ".join(info.keys())}) VALUES({", ".join(values)});'

        c = self.conn.cursor()
        c.execute(sql_statement)
        self.conn.commit()
        print(f'站点 {station_id} 的相关信息已经成功插入到数据库当中！')

    def __del__(self):
        """
        类被回收时，做一些收尾工作
        :return:
        """
        # 关闭数据库连接
        self.conn.close()

    def fetch_station_data(self, station_id: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        从数据库当中获取某个站点的历史数据
        :param station_id: 气象站点的id
        :param start_date: 历史数据的起始日期，包括当天，留空表示无限制
        :param end_date: 历史数据的结束日期，包括当天，留空表示无限制
        :return: 返回以DataFrame格式存储的数据
        """
        # 首先拼接sql语句
        if start_date is not None and end_date is not None:
            sql_statement = 'SELECT * FROM data WHERE station = ' \
                        f'"{station_id}" AND (date BETWEEN "{start_date}" AND "{end_date}");'
        elif start_date is not None and end_date is None:
            sql_statement = f'SELECT * FROM data WHERE station = "{station_id}" AND date > "{start_date}";'
        elif start_date is None and end_date is not None:
            sql_statement = f'SELECT * FROM data WHERE station = "{station_id}" AND date < "{end_date}";'
        elif start_date is None and end_date is None:
            sql_statement = f'SELECT * FROM data WHERE station = "{station_id}";'

        # 检索数据
        df_list = []
        data_rows = self.cursor.execute(sql_statement)
        for row in data_rows:
            df_list.append(row)

        # 创建数据框并返回
        return pd.DataFrame(df_list, columns=['STATION', 'DATE', 'TEMP', 'DEWP', 'SLP', 'STP', 'VISIB', 'WDSP',
                                              'MXSPD', 'GUST', 'MAX', 'MIN', 'PRCP', 'SNDP', 'FRSHTT'])

    def fetch_station_location(self, station_id: str) -> dict:
        """
        从数据库当中检索气象站的地理位置信息
        :param station_id: 气象站的id
        :return: 以字典格式存储的地理位置信息
        """
        # 构建sql语句
        sql_statement = f'SELECT * FROM info WHERE station_id = "{station_id}"'

        # 检索数据库
        self.cursor.execute(sql_statement)
        keys = ['station_id', 'name', 'latitude', 'longitude', 'country', 'province', 'city', 'district']
        if info := self.cursor.fetchone():
            return dict(zip(keys, info))
        else:
            return {}

    def fetch_chinese_data(self, province: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取我国某个省份在特定时间区段内的数据
        :param province: 省份名称，可以留空，表示获取全国所有历史数据
        :param start_date: 开始时间，可以留空，表示不设限
        :param end_date: 结束时间，可以留空，表示不设限
        :return: 返回DataFrame格式的数据
        """
        # 获取符合条件的所有气象站id
        if province:
            sql_statement = f'SELECT station_id FROM info WHERE province LIKE "%{province}%";'
        else:
            sql_statement = 'SELECT station_id FROM info WHERE country = "中国";'
        self.cursor.execute(sql_statement)
        stations_list = [item[0] for item in self.cursor.fetchall()]

        df_list = []
        for station_id in stations_list:
            print(f'正在尝试从数据库当中获取 {station_id} 的历史数据...')

            # 根据时间限定条件重新构造SQL语句
            df = self.fetch_station_data(station_id, start_date, end_date)

            # 为数据框添加站点的基本信息：站点名、省份、地市、县区
            station_info = self.fetch_station_location(station_id)
            for key in ['name', 'province', 'city', 'district']:
                df[key] = station_info[key]

            # 调整列的顺序
            df.columns = df.columns.str.lower()
            df_columns = df.columns.tolist()
            first_part_col = ['name', 'station', 'date', 'province', 'city', 'district']
            df = df[first_part_col + [item for item in df_columns if item not in first_part_col]]

            # 把所有的数据框都串在一起
            df_list.append(df)

        return pd.concat(df_list)
