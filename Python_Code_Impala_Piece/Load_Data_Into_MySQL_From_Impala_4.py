import os
import cx_Oracle
import sys
import ast


class Load_Data_Into_MySQL_From_Impala:
    def load_data(self):
        try:
            from Check_Exception import Check_Exception
            obj_exc = Check_Exception()
            anyPreviousException = obj_exc.check_exception()
            if (anyPreviousException == "found"):
                print("Exit")
                exit()

            db_mysql = cx_Oracle.connect('STDRPTS/L$AM16rj@mapls266/KYSTNDEV')
            directory = "D:\Rave\Landing_Zone_Impala_Code"
            files = os.listdir(directory)
            for file in files:
                if (file.__contains__("second_impala_return_records_length")):
                    file = open("D:\Rave\Landing_Zone_Impala_Code\\"+file, "r")
                    data = file.read()
                    spl_data = data.split("\n")
                    split_en = spl_data[0].replace("'", "").replace("[", "").replace("]", "").split(",")
                    print(split_en)
                    study = split_en[0].strip()
                    form = split_en[1].strip()
                    latest_time = split_en[2].strip()
                    sql_table = split_en[3].strip()
                    impala_table = split_en[4].strip()
                    data_len = spl_data[1]

                    if (int(data_len) > 0):
                        out_file = open(
                            "D:\Rave\Landing_Zone_Impala_Code\\impala_records_" + study + "_" + form + ".txt", "r")
                        data = out_file.read();
                        spl_data = data.split("\n")
                        print("Inserting data into MySQL")
                        for ins in spl_data:
                            if(ins!=""):
                                cur_mysql = db_mysql.cursor()
                                if(ins.__contains__("delete from ")):
                                    spl_ins = ins.split("***")
                                    lis = ast.literal_eval(spl_ins[1])
                                    print((ins))
                                    cur_mysql.executemany(spl_ins[0],lis)
                                else:
                                    print(ins)
                                    cur_mysql.execute(ins.encode('cp1252'))
                    else:
                        print("No new Data Found")
                    db_mysql.commit()
        except:
            c = (sys.exc_info()[2])
            print(c.tb_frame.f_code)
            print("Exception")
            exc = open("D:\Rave\Landing_Zone_Impala_Code\\exception.txt", "w")
            exc.write("Exception")
            exc.close();

    if __name__ == '__main__':
        load_data("")