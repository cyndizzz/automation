from snowflake.connector import ProgrammingError

def sql_execute(filename,cur):
    fd=open(filename, 'r')
    sqlFile=fd.read()
    fd.close()
    # print(sqlFile)

    sqlCommands=sqlFile.split(';')
    # display(sqlCommands)

    for command in sqlCommands:
        try:
            cur.execute(command)
            rows = cur.fetchall()
            print(rows)
        except ProgrammingError as e:
            print("Error: ", e)
            print("Command skipped: ")
