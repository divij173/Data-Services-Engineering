from datetime import datetime, time, timedelta
from flask import Flask, request, send_file
from flask_restx import Api, Resource, fields, reqparse
from flask_sqlalchemy import SQLAlchemy
import json
import requests
import pandas as pd
import shapely
import matplotlib.pyplot as plt
import geopandas as geopd
import sys


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///z5378269.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
api = Api(app,default="MyCalendar",
          title="MyCalendar Dataset",
          description="This is a time-management and scheduling calendar service")

#Load external files
georef = pd.read_csv(sys.argv[1], delimiter =";", on_bad_lines = 'skip')
cities = pd.read_csv(sys.argv[2], delimiter =",", on_bad_lines = 'skip')

#Event Model to define a structure to be stored and retrieved 
event_model = api.model('Event', {
    'name': fields.String(required=True),
    'date': fields.String(required=True),
    'from': fields.String(required=True),
    'to': fields.String(required=True),
    'location': fields.Nested(api.model('Location', {
        'street': fields.String(required=True),
        'suburb': fields.String(required=True),
        'state': fields.String(required=True),
        'post-code': fields.String(required=True)
    })),
    'description': fields.String(required=True)
})

#Event Model to store data in our database
class EventModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    date = db.Column(db.String(10), nullable=False)
    start_time = db.Column(db.String(8), nullable=False)
    end_time = db.Column(db.String(8), nullable=False)
    street = db.Column(db.String(100), nullable=False)
    suburb = db.Column(db.String(100), nullable=False)
    state = db.Column(db.String(100), nullable=False)
    post_code = db.Column(db.String(10), nullable=False)
    description = db.Column(db.String(1000))
    last_update = db.Column(db.String(19), nullable=False)
    
    def __repr__(self):
        return f"Event(name={self.name}, date={self.date}, start_time={self.start_time}, end_time={self.end_time}, street={self.street}, suburb={self.suburb}, state={self.state}, post_code={self.post_code}, description={self.description})"

# db.create_all()
@app.before_first_request
def initialize_database():
    db.create_all()
    
# Function to check for overlaps
def checkForOverlaps(data):
    start_time = data['from']
    end_time = data['to']
    overlapping_events = EventModel.query.filter(
        (EventModel.date == data['date']) &
        (EventModel.start_time < end_time) &
        (EventModel.end_time > start_time)
    ).all()
    if overlapping_events:
            return True
    return False
    
# Function to check for public holidays
def checkPublicHoliday(date):
    holidays=requests.get("https://date.nager.at/api/v2/publicholidays/2023/AU").json()
    for holiday in holidays:
        holiday_date = datetime.strptime(holiday['date'], '%Y-%m-%d')
        if holiday_date.date() == date:
            return holiday['name']
    return None

@api.route('/events/<int:event_id>')
class EventListGetDelPut(Resource):
    @api.response(404, 'Event was not found')
    @api.response(200, 'Event data retrieved Successful')
    @api.doc(description="Get an event by its ID")
    def get(self, event_id):
        event = EventModel.query.filter_by(id=event_id).first()
        if not event:
            return {'message': 'Event not found'}, 404
        
        event_date = datetime.strptime(event.date, '%d-%m-%Y').date()
        is_weekend = event_date.weekday() >= 5
        # check for holidays
        is_holiday = checkPublicHoliday(event_date)
        
        # Check previous event
        previous_event = EventModel.query.filter((EventModel.date <= event.date) & (EventModel.id != event.id)).order_by(EventModel.date.desc(), EventModel.start_time.desc()).first()
        # Check next event
        next_event = EventModel.query.filter((EventModel.date >= event.date) & (EventModel.id != event.id)).order_by(EventModel.date, EventModel.start_time).first()
        
        suburb_names = [event.suburb] + [event.suburb + ' (NSW)'] + [event.suburb + ' (QLD)'] + [event.suburb + ' (WA)'] + [event.suburb + ' (NT)'] + [event.suburb + ' (SA)'] + [event.suburb + ' (TAS)'] + [event.suburb + ' (ACT)'] + [event.suburb + ' (VIC)']

        result_df = georef[georef['Official Name Suburb'].isin(suburb_names)]
        l1=list(result_df['Geo Point'])
        l2=l1[0].split(", ")
        lat, lon = l2[0],l2[1]
        response_data = requests.get(f"https://www.7timer.info/bin/civil.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0").json()
        
        combine_date_time = datetime.strptime(event.date + ' ' + event.start_time, '%d-%m-%Y %H:%M:%S')
        get_hours = (combine_date_time - datetime.now()).total_seconds() / 3600
        response_data1 = response_data['dataseries']
        metadata={}
        if get_hours <= 168:
            for tpoint in response_data1:
                if tpoint['timepoint'] <= get_hours:
                    actual_timepoint=tpoint
            
            metadata={
            'wind-speed': str(actual_timepoint['wind10m']['speed']) +" KM",
            'weather': actual_timepoint['weather'],
            'humidity': actual_timepoint['rh2m'],
            'temperature': str(actual_timepoint["temp2m"]) + "°C",
            'holiday': is_holiday if is_holiday else 'No',
            'weekend': is_weekend
        }
        else:
            metadata={
            'holiday': is_holiday if is_holiday else 'No',
            'weekend': is_weekend
        }
            
        # Metadata information
        response = {
        'id': event.id,
        'last_update': event.last_update,
        'name': event.name,
        'date': event.date,
        'from': event.start_time,
        'to': event.end_time,
        'location': {
            'street': event.street,
            'suburb': event.suburb,
            'state': event.state,
            'post-code': event.post_code
        },
        'description': event.description,
        '_metadata': metadata,
        '_links': {
            'self': {'href': f'/events/{event.id}'},
            'previous': {'href': f"/events/{previous_event.id}"} if previous_event else "None",
            'next': {'href': f"/events/{next_event.id}"} if next_event else "None"
        }
        }
        
        return response, 200
    
    @api.response(404, 'Event was not found')
    @api.response(200, 'Successful')
    @api.doc(description="Delete an event by its ID")
    def delete(self,event_id):
        event = EventModel.query.filter_by(id=event_id).first()
        if not event:
            return {'message': 'Event not found'}, 404
        db.session.delete(event)
        db.session.commit()
        return {'message': 'The event with id {} was removed from the database!'.format(event_id), 'id': event_id}, 200
    
    
    @api.response(404, 'Event was not found')
    @api.response(400, 'Validation Error')
    @api.response(200, 'Successful')
    @api.expect(event_model, validate=True)
    @api.doc(description="Update an event by its ID")
    def patch(self,event_id):
        event = EventModel.query.filter_by(id=event_id).first()
        if not event:
            return {'message': 'Event not found'}, 404
        
        data = request.json
        for key in data:
            if key not in event_model.keys():
                return {"message": "Property {} is invalid".format(key)}, 400

        if 'name' in data:
            event.name=data['name']
        if 'date' in data:
            event.date=data['date']
        if 'from' in data:
            event.start_time=data['from']
        if 'to' in data:
            event.end_time=data['to']
        if 'street' in data['location']:
            event.street=data["location"]["street"]
        if 'suburb' in data['location']:
            event.suburb=data["location"]["suburb"]
        if 'state' in data['location']:
            event.state=data['location']['state']
        if 'post-code' in data['location']:
            event.post_code=data['location']['post-code']
        if 'description' in data:
            event.description=data['description']
        event.last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        db.session.commit()
        
        response = {
        'id': event.id,
        'last-update': event.last_update,
        '_links': {
            'self': {'href': f'/events/{event.id}'}
        }
        }
        return response, 200
        
@api.route('/events') 
class EventListPostGet(Resource):   
    global parser3
    parser3=reqparse.RequestParser()
    parser3.add_argument('order', type=str, default='+id')
    parser3.add_argument('page', type=str, default='1')
    parser3.add_argument('size', type=str, default='10')
    parser3.add_argument('filter', type=str, default='id,name')
    @api.expect(parser3)
    @api.response(400, 'No Data available for this page number')
    @api.response(404, 'Filters not found in database')
    @api.response(200, 'Event data retrieved Successful')
    @api.doc(description="Get all the events with optional information")
    def get(self):
        args=parser3.parse_args()
        event_order = args.get('order')
        event_filter= args.get('filter')
        page = int(args.get('page'))
        page_size = int(args.get('size'))

        filter1=event_filter.split(",")
        events = EventModel.query.all()
        even1=[]
        n=0
        
        event_list=[]
        for i in events:
            resp={}
            resp['id']=i.id
            resp['name']=i.name
            resp['date']=i.date
            resp['from']=i.start_time
            resp['to']=i.end_time
            resp['street']=i.street
            resp['suburb']=i.suburb
            resp['state']=i.state
            resp['post-code']=i.post_code
            resp['description']=i.description
            resp['last-update']=i.last_update
            event_list.append(resp)
        
        # Event List filters operation
        order1=event_order.split(",")
        if len(order1)>1:
            order1.reverse()
        for i in order1:
            if i== '+id':
                event_list.sort(key = lambda x:x['id'])
            
            if i == '-id':
                event_list.sort(key = lambda x:x['id'])
                event_list.reverse()
                    
            if i== '+name':
                event_list.sort(key = lambda x:x['name'])
                    
            if i== '-name':
                event_list.sort(key = lambda x: x['name'])
                event_list.reverse()
                    
            if i== '+datetime':
                event_list.sort(key = lambda x: datetime.strptime(x['date'] + ' ' + x['start_time'], '%d-%m-%Y %H:%M:%S'))
                      
            if i== '-datetime':
                event_list.sort(key = lambda x: datetime.strptime(x['date'] + ' ' + x['start_time'], '%d-%m-%Y %H:%M:%S'))
                event_list.reverse()
                
            if i== '+date':
                event_list.sort(key = lambda x: datetime.strptime(x['date'], '%d-%m-%Y'))
            
            if i == '-date':
                event_list.sort(key = lambda x: datetime.strptime(x['date'], '%d-%m-%Y'))
                event_list.reverse()
                
            if i== '+from':
                event_list.sort(key = lambda x: datetime.strptime(x['from'], '%H:%M:%S'))
            
            if i == '-from':
                event_list.sort(key = lambda x: datetime.strptime(x['from'], '%H:%M:%S'))
                event_list.reverse()
                
            if i== '+to':
                event_list.sort(key = lambda x:datetime.strptime(x['to'], '%H:%M:%S'))
            
            if i == '-to':
                event_list.sort(key = lambda x:datetime.strptime(x['to'], '%H:%M:%S'))
                event_list.reverse()
                
            if i== '+street':
                event_list.sort(key = lambda x:x['street'])
            
            if i == '-street':
                event_list.sort(key = lambda x:x['street'])
                event_list.reverse()
                
            if i== '+suburb':
                event_list.sort(key = lambda x:x['suburb'])
            
            if i == '-suburb':
                event_list.sort(key = lambda x:x['suburb'])
                event_list.reverse()
            
            if i== '+state':
                event_list.sort(key = lambda x:x['state'])
            
            if i == '-state':
                event_list.sort(key = lambda x:x['state'])
                event_list.reverse()
                
            if i== '+post-code':
                event_list.sort(key = lambda x:x['post-code'])
            
            if i == '-post-code':
                event_list.sort(key = lambda x:x['post-code'])
                event_list.reverse()
                
            if i== '+description':
                event_list.sort(key = lambda x:x['description'])
            
            if i == '-description':
                event_list.sort(key = lambda x:x['description'])
                event_list.reverse()
                
            if i== '+last-update':
                event_list.sort(key = lambda x:datetime.strptime(x['last-update'], '%Y-%m-%d %H:%M:%S'))
            
            if i == '-last_update':
                event_list.sort(key = lambda x:datetime.strptime(x['last-update'], '%Y-%m-%d %H:%M:%S'))
                event_list.reverse()
        
        even1=[]
        if len(event_list)<(page-1)*page_size:
            return {"message":'No Data available for this page number'}, 400

        temp=((page-1)*page_size)
        for j in range(1+temp,temp+1+page_size):
            n+=1
            if len(event_list)<j:
                break
            res2={}
            for filts in filter1:
                if filts not in event_list[j-1]:
                    return {"message": "Filters not found in database"}, 404
                res2[filts]=event_list[j-1][filts]
            even1.append(res2)
        # print(event_list)
        if n+page_size*(page-1)<len(event_list):
            n=page+1
        else:
            n=page
        response={
            "page": page,
            "page-size":page_size,
            "events":even1,
            "_links": {
                "self": {
                    "href": f"/events?order={event_order}&page={page}&size={page_size}&filter={event_filter}"
                },
                "next": {
                  "href": f"/events?order={event_order}&page={n}&size={page_size}&filter={event_filter}"
                }
            }
        }

        return response, 200
    
    
    @api.response(201, 'Data added Successfully')
    @api.response(400, 'Validation Error')
    @api.doc(description="create a new event")
    @api.expect(event_model)
    def post(self):
        data = api.payload
        
        if checkForOverlaps(data):
            return {'message': 'Event overlaps with existing event'}, 400
   
        # Add new event to database
        new_event = EventModel(
            name=data['name'],
            date=data['date'],
            start_time=data['from'],
            end_time=data['to'],
            street=data['location']['street'],
            suburb=data['location']['suburb'],
            state=data['location']['state'],
            post_code=data['location']['post-code'],
            description=data['description'],
            last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        )
        db.session.add(new_event)
        db.session.commit()
        
        response = {
            'id': new_event.id,
            'last-update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            '_links': {
                'self': {
                    'href': f'/events/{new_event.id}'
                }
            }
        }
        return response, 201
    
    
    
    
@api.route('/events/statistics') 
class EventListGetStat(Resource):
    global parser1
    parser1=reqparse.RequestParser()
    parser1.add_argument('format', type=str, choices=('json', 'image'), default='json')
    @api.expect(parser1)
    @api.response(404, 'Event was not found')
    @api.response(400, "Invalid format parameter. Must be 'json' or 'image'")
    @api.response(200, 'Event data retrieved Successful')
    @api.doc(description="Get an event statistics by its ID")
    def get(self):
        args=parser1.parse_args()
        format1 = args.get('format')
        
        events = EventModel.query.all()
        if not events:
            return {'message': 'Event not found'}, 404
        
        # Statistics data
        total_events = len(events)
        today = datetime.now().date()
        week1 = today - timedelta(days=today.weekday())
        week2 = week1 + timedelta(days=6)
        month1 = datetime(today.year, today.month, 1).date()
        month2 = datetime(today.year, today.month+1, 1).date() - timedelta(days=1)
        events_freq = {}
        for event in events:
            date = datetime.strptime(event.date, '%d-%m-%Y').date()
            if date in events_freq:
                events_freq[date] += 1
            else:
                events_freq[date] = 1
        tc_week = sum(1 for event in events if datetime.strptime(event.date, '%d-%m-%Y').date() >= week1 and datetime.strptime(event.date, '%d-%m-%Y').date() <= week2)
        tc_month = sum(1 for event in events if datetime.strptime(event.date, '%d-%m-%Y').date() >= month1 and datetime.strptime(event.date, '%d-%m-%Y').date() <= month2)
        
        formatted_dates = {date.strftime('%d-%m-%Y'): events_freq[date] for date in events_freq}
        sorted_dict = dict(sorted(formatted_dates.items(), key=lambda x: datetime.strptime(x[0], '%d-%m-%Y')))
        if format1 == 'json':
            
            return {
                "total": total_events,
                "total-current-week": tc_week,
                "total-current-month": tc_month,
                "per-days": sorted_dict
            }, 200
        elif format1 == 'image':
            fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(15, 11))
            # Line chart for events per day
            ax1.plot(sorted_dict.keys(), sorted_dict.values())
            ax1.tick_params(axis='x', labelrotation=45)
            ax1.set_title("Number of Events per Day")
            ax1.set_xlabel("Date")
            ax1.set_ylabel("Number of Events")
            # Pie chart for total events
            labels = ["Current Month", "Other Months"]
            sizes = [tc_month, total_events - tc_month]
            ax2.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
            ax2.set_title("Total Number of Events")
            
            fig.suptitle('Calendar Statistics', fontsize=16)
            plt.savefig('calendar_statistics.png')
            plt.close()
            return send_file('calendar_statistics.png', mimetype='image/png')
        else:
            return f"Invalid format{format1} parameter. Must be 'json' or 'image'.", 400

@api.route('/weather') 
class EventListGetWeather(Resource):
    global parser2
    parser2=reqparse.RequestParser()
    parser2.add_argument('date', type=str)
    @api.expect(parser2)
    @api.response(400, 'Date provided should be within 7 days')
    @api.response(200, 'Event data retrieved Successful')
    @api.doc(description="Get Weather Info as an image")
    def get(self):
        args=parser2.parse_args()
        future_date = args.get('date')
        today = datetime.today().strftime("%d-%m-%Y")
        today = datetime.strptime(today, '%d-%m-%Y').date()

        future_date = datetime.strptime(future_date, '%d-%m-%Y').date()
        num_days = (future_date - today).days
        cityDict = pd.DataFrame()
        if num_days*24 <= 168:
            city_names = ["Sydney", "Canberra", "Hobart", "Melbourne", "Adelaide", "Perth", "Alice Springs", "Broome", "Darwin", "Cairns", "Brisbane"]
            
            for city in city_names:
                result_df = cities[cities['city']==(city)]
                l1=str(result_df['lat']).split(" ")
                lat=l1[3].split("\n")[0]
                l2=str(result_df['lng']).split(" ")
                lon=l2[4].split("\n")[0]
                response_data = requests.get(f"https://www.7timer.info/bin/civil.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0").json()
                response_data1 = response_data['dataseries']
                for tpoint in response_data1:
                    if tpoint['timepoint'] <= num_days*24:
                        actual_timepoint=tpoint
                
                if city != 'Perth':
                    new_row={'city': city + " ," + str(actual_timepoint["temp2m"]) + "°C", 'lat': float(lat), 'lon': float(lon)}
                    cityDict=cityDict.append(new_row, ignore_index=True)
            
            georef1=georef.drop(9296)
            shaped_list=[]
            for ind, row in georef1.iterrows():
                shaped=shapely.geometry.shape(json.loads(row['Geo Shape']))
                shaped_list.append(shaped)
            shaped_df=pd.DataFrame(shaped_list)
            shaped_df.columns=['Shape']
            
            gdFrame=geopd.GeoDataFrame(shaped_df, geometry='Shape', crs=4326)

            gdCityFrame=geopd.GeoDataFrame(cityDict, geometry=geopd.points_from_xy(x=cityDict.lon, y=cityDict.lat), crs=4326)
 
            fig,ax=plt.subplots(figsize=(15,15))
            ax.scatter(cityDict.lon,cityDict.lat, color='red')
            for idx, row in cityDict.iterrows():
                ax.text(row.lon,row.lat, row.city, fontsize=11, fontweight='bold', color='black')
            gdFrame.plot(ax=ax,column='Shape', alpha=0.5)
            plt.savefig('weather_forcast.png')
            plt.close()
            return send_file('weather_forcast.png', mimetype='image/png')
        else:
            return {"message": "Date provided should be within 7 days"}, 400
              
        
if __name__ == '__main__':
    app.run(debug=True)
