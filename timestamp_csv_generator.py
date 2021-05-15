from random import randint
import datetime
import time
import uuid


file_id=1


# For writing timestam and a random value at regular intervals to a file
try:
    while True:
        with open(f"data/streaming-data/event-time-data/{file_id}.csv", 'w') as f:
            for i in range(5):
                rec=f"{str(uuid.uuid1().int)}\t{datetime.datetime.now().isoformat()}\t{randint(0,100)}\n"
                print(rec)
                f.write(rec)

            file_id+=1
            time.sleep(1)



except KeyboardInterrupt:
    pass