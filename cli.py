from db import DB
from option import DBOption, WriteOption, ReadOption
from status import Status
from write_batch import WriteBatch
from snapshot import Snapshot

if __name__ == '__main__':
    option = DBOption()
    option.create_if_missing = True
    option.only_mem = True

    db_name = str(input('Enter database name:\n>>> '))
    db, s = DB.open(db_name, option)
    if not s.ok():
        print('Error:', s.msg)
        exit(-1)

    while True:
        raw_input = str(input('> '))
        cmd = raw_input.split(' ')
        if len(cmd) == 0:
            continue

        if cmd[0] == 'put':
            if len(cmd) % 2 != 1 or len(cmd) == 1:
                print('Error: Invalid put command: wrong number of arguments')
                continue
            batch = WriteBatch()
            for i in range(1, len(cmd), 2):
                batch.put(cmd[i], cmd[i+1])
            db.write(WriteOption(), batch)
        elif cmd[0] == 'del':
            if len(cmd) == 1:
                print('Error: Invalid del command: no key specified')
                continue
            batch = WriteBatch()
            for i in range(1, len(cmd)):
                batch.delete(cmd[i])
            db.write(WriteOption(), batch)
        # get or get_<seq>
        elif cmd[0].startswith('get'):
            read_option = ReadOption()
            if len(cmd[0]) == 3:
                # get current version
                pass
            else:
                # specific version
                # cmd[0] == 'get_${seq}'
                if '_' not in cmd[0]:
                    print('Error: Invalid get command: no version specified')
                    continue
                split_cmd = cmd[0].split('_')
                if len(split_cmd) != 2:
                    print('Error: Invalid get command: invalid version argument: wrong format')
                    continue
                try:
                    seq = int(split_cmd[1])
                    read_option.snapshot = Snapshot(seq)
                except ValueError as e:
                    print('Error: Invalid get command: invalid version argument: not an integer')
                    continue

            for i in range(1, len(cmd)):
                value = []
                s = db.get(read_option, cmd[i], value)
                if s == Status.NotFound():
                    print('')
                elif s.ok():
                    print(value[0])
                else:
                    print('Error:', s.msg)
        elif cmd[0] == 'seq':
            print(db.versions.last_sequence())
        elif cmd[0] == 'exit':
            break
        else:
            if raw_input.strip() != '':
                print('Error: Invalid command:', cmd[0])
    print('bye~')
    db.close()



