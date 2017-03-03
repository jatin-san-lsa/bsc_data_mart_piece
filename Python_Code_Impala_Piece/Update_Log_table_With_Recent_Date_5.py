import os
import pyodbc
import cx_Oracle
import sys
class Update_Log_table_With_Recent_Date:
    def update_log(self):
        try:

            from Check_Exception import Check_Exception
            obj_exc = Check_Exception()
            anyPreviousException = obj_exc.check_exception()
            if (anyPreviousException == "found"):
                print("Exit")
                exit()

            cnxn = pyodbc.connect("DSN=edh-impala-dev", autocommit=True)
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
                    last_type_of_load = split_en[5].strip()
                    has_full_load_previous_processed = split_en[6].strip()

                    data_len = spl_data[1]
                    print((data_len))
                    if (int(data_len) > 0):
                        impala_recent_date_processed = spl_data[3]

                        print("Updating Log Data with Recent Data   "+study+"_"+form)
                        print("Previous Date was "+latest_time)

                        cur_mysql = db_mysql.cursor()
                        update_log_dates = "UPDATE edhpoc_recent_data_log SET latest_savets_time_found=" + impala_recent_date_processed + " WHERE impala_table='" + impala_table + "'"
                        print(update_log_dates)
                        print("Latest Date is: "+impala_recent_date_processed)
                        cur_mysql.execute(update_log_dates)

                        if(has_full_load_previous_processed==""):
                            update_log_dates = "UPDATE edhpoc_recent_data_log SET has_full_load_processed='Yes' WHERE form_name='" + form + "'"
                            print(update_log_dates)
                            cur_mysql = db_mysql.cursor()
                            cur_mysql.execute(update_log_dates)


                        if(form=="DVINFO"):
                            print("Calling Procedure")
                            cur_mysql = db_mysql.cursor()
                            cur_mysql.callproc("EDHPOC_DM.COMPILE_DEVIATIONS", [4])

                        db_mysql.commit()

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
            update_log("")