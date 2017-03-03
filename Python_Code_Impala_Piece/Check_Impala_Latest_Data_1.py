import pyodbc
import sys
import os
class Check_Impala_Latest_Data:
    def get_recent(self):
        try:
            in_file = open("D:\Rave\Landing_Zone_Impala_Code\\pentaho_first.txt", "r")
            data = in_file.read()
            spl_data = data.split("\n")
            for line in spl_data:
                if(line.__contains__(",")):
                    split_en = line.replace("'","").replace("[","").replace("]","").split(",")
                    print(split_en)
                    study = split_en[0].strip()
                    form = split_en[1].strip()
                    latest_time = split_en[2].strip()
                    sql_table = split_en[3].strip()
                    impala_table = split_en[4].strip()
                    last_type_of_load = split_en[5].strip()
                    has_full_load_previous_processed = split_en[6].strip()

                    conn_impala = pyodbc.connect("DSN=edh-impala-dev", autocommit=True)
                    cur_impala = conn_impala.cursor()

                    if(has_full_load_previous_processed==""):
                        print("nothing")
                        query_impala = 'SELECT * FROM clinical_tgt.' + impala_table + ' WHERE  SaveTS!="SaveTS";'
                        print(query_impala)
                        cur_impala.execute(query_impala)
                        results = cur_impala.fetchall()
                    else:
                        print("yes")
                        query_impala = 'SELECT * FROM clinical_tgt.' + impala_table + ' WHERE  SaveTS!="SaveTS" AND SaveTS > "' + latest_time + '";'
                        cur_impala.execute(query_impala)
                        results = cur_impala.fetchall()


                    print(query_impala)
                    cur_impala.execute(query_impala)
                    results = cur_impala.fetchall()
                    print("Number of new records found: "+str(len(results)))

                    file = open("D:\Rave\Landing_Zone_Impala_Code\\second_impala_return_records_length_"+study+"_"+form+".txt", "w")
                    file.write(line)
                    file.write("\n")
                    file.write(str(len(results)))
                    if(len(results)>0):
                        query_impala = 'SELECT distinct transactiontype FROM clinical_tgt.' + impala_table + ' WHERE SaveTS!="SaveTS" AND SaveTS > "' + latest_time + '";'
                        print(query_impala)
                        cur_impala.execute(query_impala)
                        results = cur_impala.fetchall()
                        file.write("\n")
                        print(results)
                        if(str(results).__contains__("('', )")):
                            print("Yes Full/Initial Load")
                            file.write("Yes")
                        else:
                            print("Not Full/Initial Load")
                            file.write("No")
                        print("Type of transactions found: " + str(len(results)))

                        get_max_date_from_table = "select MAX(SaveTS) from clinical_tgt." + impala_table + "  where SaveTS!='SaveTS'"

                        conn_impala = pyodbc.connect("DSN=edh-impala-dev", autocommit=True)
                        cur_impala = conn_impala.cursor()
                        cur_impala.execute(get_max_date_from_table)
                        results = cur_impala.fetchall()
                        impala_recent_date_processed = ""

                        for dat in results:
                            impala_recent_date_processed = str(dat).split(",")[0].replace("(", "").replace(")", "")
                            break;
                        file.write("\n")
                        file.write(impala_recent_date_processed)
                    file.close()

        except:
            c = (sys.exc_info()[2])
            print(c.tb_frame.f_code)
            print("Exception")
            exc = open("D:\Rave\Landing_Zone_Impala_Code\\exception.txt", "w")
            exc.write("Exception")
            exc.close();

    if __name__ == '__main__':
        get_recent("")