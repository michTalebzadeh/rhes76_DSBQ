from __future__ import print_function
import sys
from src.config import config, oracle_url
from pyspark.sql import functions as F
from pyspark.sql.functions import col, round
from pyspark.sql.window import Window
from sparkutils import sparkstuff as s
from othermisc import usedFunctions as uf
import locale
locale.setlocale(locale.LC_ALL, 'en_GB')
import cx_Oracle


def main():
    appName = config['common']['appName']
    spark_session = s.spark_session(appName)
    spark_context = s.sparkcontext()
    spark_context.setLogLevel("ERROR")
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nStarted at");uf.println(lst)
    oracle = RandomData(spark_session, spark_context)
    dfRandom = oracle.generateRamdomData()
    #dfRandom.printSchema()
    #dfRandom.show(20, False)
    #oracle.loadIntoOracleTable(dfRandom)

    oracle.loadIntoOracleTableWithCursor(dfRandom)
    lst = (spark_session.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ")).collect()
    print("\nFinished at");uf.println(lst)

class RandomData:
    def __init__(self, spark_session, spark_context):
        self.spark = spark_session
        self.sc = spark_context
        self.config = config

    def generateRamdomData(self):
        start = 1
        numRows = 10
        rows = 0
        end = start + numRows
        print("starting at ID = ", start, ",ending on = ", end)
        Range = range(start, end)
        ## This traverses through the Range and increment "x" by one unit each time, and that x value is used in the code to generate random data through Python functions in a class
        rdd = self.sc.parallelize(Range). \
            map(lambda x: (x, uf.clustered(x, numRows), \
                           uf.scattered(x, numRows), \
                           uf.randomised(x, numRows), \
                           uf.randomString(50), \
                           uf.padString(x, " ", 50),
                           uf.padSingleChar("x", 255)))
        df = rdd.toDF(). \
            withColumnRenamed("_1", "ID"). \
            withColumnRenamed("_2", "CLUSTERED"). \
            withColumnRenamed("_3", "SCATTERED"). \
            withColumnRenamed("_4", "RANDOMISED"). \
            withColumnRenamed("_5", "RANDOM_STRING"). \
            withColumnRenamed("_6", "SMALL_VC"). \
            withColumnRenamed("_7", "PADDING")
        return df

    def loadIntoOracleTable(self, df2):
        # write to Oracle table, all uppercase not mixed case and column names <= 30 characters in version 12.1
        tableName = "randomdata"
        fullyQualifiedTableName = self.config['OracleVariables']['dbschema']+'.'+tableName
        user = self.config['OracleVariables']['oracle_user']
        password = self.config['OracleVariables']['oracle_password']
        driver = self.config['OracleVariables']['oracle_driver']
        mode = "overwrite"
        s.writeTableWithJDBC(df2,oracle_url,fullyQualifiedTableName,user,password,driver,mode)
        print(f"""created {fullyQualifiedTableName}""")
        # read data to ensure all loaded OK
        fetchsize = self.config['OracleVariables']['fetchsize']
        read_df = s.loadTableFromJDBC(self.spark,oracle_url,fullyQualifiedTableName,user,password,driver,fetchsize)
        # check that all rows are there
        if df2.subtract(read_df).count() == 0:
            print("Data has been loaded OK to Oracle table")
        else:
            print("Data could not be loaded to Oracle table, quitting")
            sys.exit(1)

    def loadIntoOracleTableWithCursor(self, df):
        # write to Oracle table, all uppercase not mixed case and column names <= 30 characters in version 12.1
        tableName = "randomdata"
        fullyQualifiedTableName = self.config['OracleVariables']['dbschema']+'.'+tableName
        user = self.config['OracleVariables']['oracle_user']
        password = self.config['OracleVariables']['oracle_password']
        serverName = self.config['OracleVariables']['oracleHost']
        port = self.config['OracleVariables']['oraclePort']
        serviceName = self.config['OracleVariables']['serviceName']
        dsn_tns = cx_Oracle.makedsn(serverName, port, service_name=serviceName)
        conn = cx_Oracle.connect(user, password, dsn_tns)
        cursor = conn.cursor()

        for row in df.rdd.collect():
            id = row[0]
            clustered = row[1]
            scattered = row[2]
            randomised = row[3]
            random_string = row[4]
            small_vc = row[5]
            padding = row[6]
            sqlText = f"""insert into {fullyQualifiedTableName} (id,clustered,scattered,randomised,random_string,small_vc,padding,derived_col)
                      values ({id},{clustered},{scattered},{randomised},'{random_string}','{small_vc}','{padding}',cos({id}))"""
            print(sqlText)
            cursor.execute(sqlText)
            conn.commit()

if __name__ == "__main__":
  main()
