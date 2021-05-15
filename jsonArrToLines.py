import json


with open("../datasets/student-list.json","r+") as f:
    q= json.load(f)
    # print(q)
    with open("../datasets/stu-list.jsonl","w") as f2:
        for i in q:
            f2.write(f"{json.dumps(i)}\n")
            
