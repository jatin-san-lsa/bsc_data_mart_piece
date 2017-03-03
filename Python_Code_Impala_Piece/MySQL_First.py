import pymysql


class MySQL:
    def get_tables(self):
        db_mysql = pymysql.connect(host="localhost",  # your host, usually localhost
                             user="root",  # your username
                             passwd="hello123",  # your password
                             db="EDHPOC")  # name of the data base

        in_file = open("C:\\Users\\jatin\\Desktop\\BSC\\pentaho_first.txt", "r")

        data = in_file.read()
        spl_data = data.split("\n")
        split_en = spl_data[0].split(",")
        study = split_en[0]
        form = split_en[1]
        impala_table = split_en[2]
        latest_time = split_en[3]
        sql_table = split_en[4]


        for imp  in spl_data:

            cur_mysql = db_mysql.cursor()
            cur_mysql.execute("SELECT * FROM recent_date_log")

            file = open("C:\\Users\\jatin\\Desktop\\BSC\\first_sql_list.txt", "w")

            for row in cur_mysql.fetchall():
                file.write(row[0]+","+row[1]+","+row[2]+","+row[3]+","+row[4])
                file.write("\n")

        file.close()
        db_mysql.close()

    if __name__ == '__main__':
        get_tables("")