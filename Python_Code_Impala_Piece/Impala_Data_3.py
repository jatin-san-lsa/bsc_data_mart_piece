import os
import cx_Oracle
import sys
import pyodbc

class Impala_Data:
    def get_data(self):
        try:

            from Check_Exception import Check_Exception
            obj_exc = Check_Exception()
            anyPreviousException = obj_exc.check_exception()
            if (anyPreviousException == "found"):
                print("Exit")
                exit()

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
                    last_type_of_load = split_en[5].strip()
                    has_full_load_previous_processed = split_en[6].strip()

                    print(split_en)
                    data_len = spl_data[1]
                    conn_impala = pyodbc.connect("DSN=edh-impala-dev", autocommit=True)
                    cur_impala = conn_impala.cursor()

                    if(has_full_load_previous_processed==""):
                        query_impala = 'SELECT * FROM clinical_tgt.' + impala_table + ' WHERE  SaveTS!="SaveTS";'
                        cur_impala.execute(query_impala)
                        results = cur_impala.fetchall()
                    else:
                        query_impala = 'SELECT * FROM clinical_tgt.' + impala_table + ' WHERE  SaveTS!="SaveTS" AND SaveTS > "' + latest_time + '";'
                        cur_impala.execute(query_impala)
                        results = cur_impala.fetchall()


                    #print(results)
                    print("Getting data from Impala")
                    out_file = open("D:\Rave\Landing_Zone_Impala_Code\\impala_records_"+study+"_"+form+".txt", "w")

                    if (int(data_len) > 0):
                        for line in results:
                            if(str(line).__contains__("8662297")):
                                print(str(line))
                            spl_line = str(line).replace(" None,", "' ',").replace("None)", ")").replace("',", "|").replace(
                                '",', "|").replace("'", "").split("|")
                            recordid = (spl_line[25]).strip()
                            #print("))))))")
                            #print(recordid)

                            if(recordid==""):
                                print("In Record Id")
                                print(spl_line)
                                continue;

                            if (spl_line[0].replace("(", "").strip() == ""):
                                if (str(spl_line).__contains__("8662297")):
                                    print(str(spl_line))

                                out_file.write("insert into " + sql_table + " values(")
                                len_spl = len(spl_line)
                                for i in range(1, len_spl - 1):
                                    col = spl_line[i].strip()
                                    out_file.write("'" + spl_line[i].strip() + "',")
                                out_file.write("'" + spl_line[len_spl - 1].replace(")", "'").strip())
                                out_file.write(")")
                                out_file.write("\n")

                            if (spl_line[0].replace("(", "").strip() == "U"):
                                out_file.write("delete from " + sql_table + ' where "recordid" = :rid'+"***[{'rid':'"+recordid+"'}]")
                                out_file.write("\n")

                                out_file.write("insert into " + sql_table + " values(")
                                len_spl = len(spl_line)
                                for i in range(1, len_spl - 1):
                                    out_file.write("'" + spl_line[i].strip() + "',")
                                out_file.write("'" + spl_line[len_spl - 1].replace(")", "'").strip())
                                out_file.write(")");out_file.write("\n");

                            if (spl_line[0].replace("(", "").strip() == "D"):
                                recordid = (spl_line[25]).strip()
                                out_file.write("delete from " + sql_table + ' where "recordid" = :rid'+"***[{'rid':'"+recordid+"'}]")
                                out_file.write("\n")

                            if (spl_line[0].replace("(", "").strip() == "I"):
                                out_file.write("insert into " + sql_table + " values(")
                                len_spl = len(spl_line)
                                for i in range(1, len_spl - 1):
                                    col = spl_line[i].strip()
                                    out_file.write("'" + spl_line[i].strip() + "',")
                                out_file.write("'" + spl_line[len_spl - 1].replace(")", "'").strip())
                                out_file.write(")")
                                out_file.write("\n")

                    else:
                        print("No new Data Found")

                    out_file.close()
        except:
            c = (sys.exc_info()[2])
            print(c.tb_frame.f_code)
            print("Exception")
            exc = open("D:\Rave\Landing_Zone_Impala_Code\\exception.txt", "w")
            exc.write("Exception")
            exc.close();

    if __name__ == '__main__':
        get_data("")