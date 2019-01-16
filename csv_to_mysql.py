import csv
import MySQLdb as db
import logging
from logging import handlers
import sys
import time

CSV_PATH = "./csv_file/BI - App Basic Info Tracker - App Basic Info.csv"


class Logger(object):
    level_relations = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    def __init__(self,filename,level='info',when='D', backCount=3,
                 fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        self.logger = logging.getLogger(filename)
        format_str = logging.Formatter(fmt)
        self.logger.setLevel(self.level_relations.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format_str)
        th = handlers.TimedRotatingFileHandler(filename=filename,when=when,backupCount=backCount,encoding='utf-8')
        th.setFormatter(format_str)
        self.logger.addHandler(sh)
        self.logger.addHandler(th)


log = Logger('./log/BI.log', level='debug')
log_error = Logger('./log/BI_error.log', level='error')


class MysqlClient(object):
    def __init__(self, host, port, username, passwd, db_name):
        self.conn = db.connect(host=host, port=port, user=username, passwd=passwd, db=db_name, charset="utf8")
        self.cursor = self.conn.cursor()

    def app_code_exist(self, app_code, table):
        try:
            log.logger.info("app_code_exist for app_code:%s in table:%s", app_code, table)
            sql = 'SELECT * FROM %s WHERE app_code="%s"' %(table, app_code)
            result = self.cursor.execute(sql)
            if result:
                return True
            else:
                return False
        except Exception, e:
            log.logger.error(e)
            log.logger.error("ERROR HAPPEN:app_code_exist for app_code:%s in table:%s", app_code, table)
            log_error.logger.error("ERROR HAPPEN:app_code_exist for app_code:%s in table:%s", app_code, table)
            raise e

    def update_apple_id_pk(self, app_code, apple_id_pk, platform):
        try:
            log.logger.info("update_apple_id_pk for app_code:%s and apple_id_pk:%s", app_code, apple_id_pk)
            if platform == "Apple Appstore":
                sql = 'UPDATE %s SET apple_id_pk_bool=1, apple_id_pk="%s" WHERE app_code="%s"' % ("login_project",
                                                                                                     apple_id_pk,
                                                                                                     app_code)
            elif platform == "Google Play Appstore":
                sql = 'UPDATE %s SET apple_id_pk_bool=1 WHERE app_code="%s"' % ("login_project", app_code)
            else:
                raise Exception("platform parameter error for app code:%s, platform:%s" %(app_code, platform))

            log.logger.info(sql)
            result = self.cursor.execute(sql)
            self.conn.commit()
            log.logger.info("update apple id or pk bool successfully for app code:%s", app_code)
        except Exception, e:
            self.conn.rollback()
            log.logger.error(e)
            log.logger.error("ERROR HAPPEN: update_apple_id_pk for app code:%s and apple_id_pk" % app_code, apple_id_pk)
            log_error.logger.error("ERROR HAPPEN: update_apple_id_pk for app code:%s and apple_id_pk" % app_code,
                                   apple_id_pk)
            raise e

    def update_flurry_key(self, app_code, flurry_key):
        try:
            log.logger.info("update_flurry_key for app_code:%s and flurry_key:%s", app_code, flurry_key)
            sql_project = 'SELECT * FROM %s where app_code="%s"' % ("login_project", app_code)
            self.cursor.execute(sql_project)
            response = self.cursor.fetchall()
            project_id = response[0][0]
            sql_version = 'UPDATE %s SET flurry_key_bool=1, flurry_key="%s" where project_id="%s"' %("login_version",
                                                                                                        flurry_key,
                                                                                                        project_id)

            log.logger.info("select:project " + sql_project)
            log.logger.info("update:version " + sql_version)
            self.cursor.execute(sql_version)
            self.conn.commit()
            log.logger.info("update flurry key successfully for app code:%s", app_code)
        except Exception, e:
            self.conn.rollback()
            log.logger.error(e)
            log.logger.error("ERROR HAPPEN: update_apple_id_pk for app code:%s and flurry_key" % app_code, flurry_key)
            log_error.logger.error("ERROR HAPPEN: update_apple_id_pk for app code:%s and flurry_key" % app_code,
                                   flurry_key)
            raise e

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    file_tmp = open(CSV_PATH, "r")
    reader = csv.reader(file_tmp)
    duplicated = list()

    csv_result = dict()
    for item in reader:
        if reader.line_num == 1:
            pass
        else:
            if csv_result.get(item[2], None) is None:
                csv_result[item[2]] = dict()
                csv_result[item[2]]["flurry_key"] = item[3]
                csv_result[item[2]]["apple_id_pk"] = item[4]
                csv_result[item[2]]["platform"] = item[1]
            else:
                log.logger.error("Already exist app code:%s" % item[2])
                duplicated.append(item[2])

    # to check app code
    # for key, item in csv_result.items():
    #     if item["platform"] == "Google Play Appstore":
    #         if not key.endswith("G"):
    #             print "app code not match for app code:%s" %key
    #
    #     if item["platform"] == "Apple Appstore":
    #         if key.endswith("G"):
    #             print "app code not match for app code:%s" %key
    # to check platform
    # result_platform = [item["platform"] for key, item in csv_result.items()]
    # print result_platform
    # print set(result_platform)

    if duplicated:
        log.logger.error("csv data verify failed")
        sys.exit(1)
    else:
        log.logger.info("csv data verify successfully")

    mysql_cli = MysqlClient("172.16.126.63", 3306, "db_pts_nexa", "db_pts_nexa", "db_pts_nexa")
    try:
        if csv_result:
            for app, value in csv_result.items():
                app_code = app
                flurry_key = value["flurry_key"]
                apple_id_pk = value["apple_id_pk"]
                platform = value["platform"]
                if mysql_cli.app_code_exist(app, "login_project"):
                    mysql_cli.update_apple_id_pk(app_code, apple_id_pk, platform)
                    mysql_cli.update_flurry_key(app_code, flurry_key)
                else:
                    log.logger.warning("Not exist: app_code_exist for app_code:%s in table:%s",
                                       app_code, "login_project")
            time.sleep(0.1)
    except Exception, e:
        raise e

    finally:
        mysql_cli.close()
