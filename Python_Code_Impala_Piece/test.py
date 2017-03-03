import pyodbc
import cx_Oracle
conn_impala = cx_Oracle.connect('STDRPTS/L$AM16rj@mapls266/KYSTNDEV')
cur_impala = conn_impala.cursor()

cur_impala.callproc("EDHPOC_DM.COMPILE_DEVIATIONS",[1])

conn_impala.commit();