import redis
import json
r = redis.Redis(host='localhost', port=6379, db=0)
key = 'view_range:klemen.zivkovic@gmail.com'
data = r.get(key)
if data:
    vr = json.loads(data)
    print('View range:', vr)
else:
    print('No view_range set')