import requests
import redis
import json
import time

try:
    # Start login
    response = requests.post('http://localhost:5000/start-login', json={'email': 'klemen.zivkovic@gmail.com'})
    print('Start login response:', response.status_code, response.text)

    # Wait a bit
    time.sleep(1)

    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    print('Redis connected')

    # Get all magic_token keys
    keys = r.keys('magic_token:*')
    print('Magic token keys:', keys)

    for key in keys:
        value = r.get(key)
        if value:
            data = json.loads(value)
            print('Key:', key, 'Data:', data)
            if data.get('email') == 'klemen.zivkovic@gmail.com':
                token = key.decode().split(':', 1)[1]
                print('Token found:', token)
                break
    else:
        print('Token not found')
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()