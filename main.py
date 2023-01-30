import json
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins = ['http://localhost:3000'],
    allow_methods = ['*'],
    allow_headers = ['*']
)

redis = get_redis_connection(
    host='redis-12854.c305.ap-south-1-1.ec2.cloud.redislabs.com',
    port = 12854,
    password = 'PF295aCSYuXw6AY3kfJY9P7L9ceh3yTM',
    decode_responses = True
)   

# Writing the models for the app

# Tracking assigning of Deliveries
class Delivery(HashModel):
    budget: int = 0
    notes : str = ''

    class Meta:
        database = redis

# Event Tracking model
class Event(HashModel):
    delivery_id: str = None
    type: str
    data: str

    class Meta:
        database = redis

# Post method to add data to redis db for Delivery model
@app.post('/deliveries/create')
async def create(request: Request):
    body = await request.json()

    # As we are sending event data about Deliveries in a Json called data, we index on ['data']['budget']
    # When we are doing .save(), we are saving the data in Redis Json DB not in the Redis Cache
    delivery = Delivery(budget = body['data']['budget'],notes = body['data']['notes']).save()

    # Sending the event to the DB
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data']))
    return event
