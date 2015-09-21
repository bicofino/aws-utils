#!/usr/bin/env python
# coding: utf-8
# vim: tabstop=2 noexpandtab
"""
    Author: Danilo F. Chilene
    Email:      bicofino at gmail dot com
"""

import logging
import os
import datetime
from boto import connect_s3
from boto.s3.key import Key
from boto.exception import S3ResponseError
from datetime import timedelta

# Configuration

AWS_ACCESS_KEY_ID = '*****'
AWS_SECRET_ACCESS_KEY = '*****'
REGION = 'us-east-1'
BUCKET = 'mybucket'
DATABASES = ['mysql', 'otherdatabase']
BACKUP_DIR = '/tmp'
today = datetime.date.today()
previous = today - timedelta(days=7)
mysql_hostname = 'hostname'
mysql_username = 'youruser'
mysql_password = '*******'
mysql_dump_path = '/usr/bin/mysqldump'


class S3Backup():

    def connect(self):
        """ Connect to S3 Amazon Service """

        try:
            self.c = connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        except Exception, e:
            logging.error("Exception getting S3 connection: {0}".format(e))
        return self.c

    def create_bucket(self):
        try:
            bucket = self.c.get_bucket(BUCKET, validate=True)
        except S3ResponseError:
            bucket = self.c.create_bucket(BUCKET)
        self.b = self.c.get_bucket(bucket)

    def backup(self):
        for d in DATABASES:
            d = d.strip()
            backup_file = '{0}/{1}-{2}.sql.gz'.format(BACKUP_DIR, d, today)
            print 'Creating archive for ' + backup_file
            os.popen(mysql_dump_path + " -u {0} -p{1} -h {2} -e --opt -c {3}|gzip -c > {4}".format(mysql_username, mysql_password, mysql_hostname, d, backup_file))
            print 'Uploading database dump to S3' + backup_file + '...'
            k = Key(self.b)
            k.Key = backup_file
            k.set_contents_from_filename(backup_file)
            k.set_acl("public-read")
