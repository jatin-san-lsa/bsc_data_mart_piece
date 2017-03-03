import os
import glob


class Update_Log_table_With_Recent_Date:
    def update_log(self):

        import os
        directory = "D:\Rave\Landing_Zone_Impala_Code"
        files = os.listdir(directory)
        for f in files:
            filename = directory+"\\\\"+f
            os.remove(filename)

    if __name__ == '__main__':
        update_log("")

