import os
import glob


class Check_Exception:
    def check_exception(self):

        import os
        directory = "D:\Rave\Landing_Zone_Impala_Code"
        files = os.listdir(directory)
        for filename in files:
            if(filename.strip()=="exception.txt"):
                return "found"
            else:
                return "not found"

