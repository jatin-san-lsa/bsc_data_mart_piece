import os
import cx_Oracle
import sys
import pyodbc

class Truncate_MySQL_Target:
    def truncate(self):
        try:
            from Check_Exception import Check_Exception
            obj_exc = Check_Exception()
            anyPreviousException = obj_exc.check_exception()
            if(anyPreviousException=="found"):
                print("Exit")
                exit()

            db_mysql = cx_Oracle.connect('STDRPTS/L$AM16rj@mapls266/KYSTNDEV')
            cnxn = pyodbc.connect("DSN=edh-impala-dev", autocommit=True)
            directory = "D:\Rave\Landing_Zone_Impala_Code"
            files = os.listdir(directory)
            for file in files:
                if (file.__contains__("second_impala_return_records_length")):
                    file = open("D:\Rave\Landing_Zone_Impala_Code\\"+file, "r")
                    data = file.read()
                    spl_data = data.split("\n")
                    split_en = spl_data[0].replace("'", "").replace("[", "").replace("]", "").split(",")
                    study = split_en[0].strip()
                    form = split_en[1].strip()
                    latest_time = split_en[2].strip()
                    sql_table = split_en[3].strip()
                    impala_table = split_en[4].strip()
                    data_len = spl_data[1]
                    print(split_en)
                    if (int(data_len) > 0):
                        is_full_load = spl_data[2].strip()
                        print(is_full_load+" Full/Initial Load")
                        if (is_full_load == "Yes"):
                            cur_impala = cnxn.cursor()
                            tbl = impala_table.upper()
                            query_impala = 'describe clinical_tgt.' + tbl + ';'
                            print(query_impala)
                            cur_impala.execute(query_impala)
                            results = str(cur_impala.fetchall())

                            # print(results)
                            col = ((results).replace("', 'string', ''), ('", "|").replace("', 'string', '')]", "").replace("[('", ""))
                            spl_col = (col.split("|"))

                            dro = 'drop table "EDHPOC_CV_' + tbl + '"'

                            print(dro)

                            str1 = """CREATE TABLE "STDRPTS"."EDHPOC_CV_""" + tbl + '"('

                            for i in spl_col:
                                if (i.strip().upper() != "transactiontype".upper()):
                                    #print(i)
                                    str1 += '"' + i + '" NVARCHAR2(1999),\n'

                            str1 = str1[:-2] + """)SEGMENT CREATION IMMEDIATE
                              PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
                             NOCOMPRESS LOGGING
                              STORAGE(INITIAL 1048576 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
                              PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
                              BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
                              TABLESPACE "STDRPTS" """

                            #print(str1)

                            #print(dro)
                            cur_mysql = db_mysql.cursor()
                            cur_mysql.execute(dro)
                            print("2222")
                            cur_mysql = db_mysql.cursor()
                            cur_mysql.execute(str1)

                            db_mysql.commit();

                    else:
                        print("No new Data Found")
        except:
            c = (sys.exc_info()[2])
            print(c.tb_frame.f_code)
            print("Exception")
            exc = open("D:\Rave\Landing_Zone_Impala_Code\\exception.txt", "w")
            exc.write("Exception")
            exc.close();

    if __name__ == '__main__':
        truncate("")