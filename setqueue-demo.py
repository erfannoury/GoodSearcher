from SetQueue import SetQueue

q = SetQueue()
q.put(2)
q.put(3)
print q.contains(2)
print q.qsize()
print q.get()
print q.get()
print q.contains(2)
