import re
from models import *


def clean(row):
    d = row2dict(row)
    for k,v in d.items():
        if k == 'id' or not v:
            continue

        # Remove non-ascii characters
        #v = unicode(v, 'utf-8').encode("ascii", "ignore")
        v = unicode(v).encode("ascii", "ignore")

        # Remove escaped html characters like &nbsp;
        v = re.sub(r'&\S+;', '', v)

        # Remove ??
        v = v.replace('??', '')

        # Remove extra whitespace
        v = ' '.join(v.split())

        # Change empty string to NULL
        v = v or None

        # Save to rec
        d[k] = v

    return d


def process_schedule():
    debug("Processing Schedule")
    db.query(Schedule).delete() # Truncate Schedule
    db.commit()

    count = db.query(func.count(TempSchedule.id)).scalar()
    for i, row in enumerate(db.query(TempSchedule.id)):
        debug("Processing schedule # %s of %s. Remaining %s" % (i, count, count-i))

        s = db.query(TempSchedule).get(row.id)
        s = clean(s)
        rec = {}
        rec['train_number'] = s['train_number']
        rec['stop_number'] = s['stop_number']
        rec['station_code'] = s['code'].upper()
        rec['station_name'] = s['station_name']
        rec['arrival'] = s['arrives']
        rec['departure'] = s['departs']
        rec['halt'] = s['halt']
        rec['day'] = s['day']
        rec['distance_travelled'] = s['km']

        db.add(Schedule(**rec))
    db.commit()


def process_stations():
    debug("Processing Stations")
    db.query(Station).delete() # Truncate Station
    db.commit()

    count = db.query(func.count(distinct(TempSchedule.code))).scalar()
    for i, row in enumerate(db.query(TempSchedule.code).distinct()):
        code = row.code
        print "Processing station %s (%s of %s)" % (code, i, count)
        s = db.query(TempSchedule).filter_by(code=code).first()
        s = clean(s)

        rec = {}
        rec['code'] = s['code']
        rec['name'] = s['station_name']
        rec['zone'] = s['zone']

        address = s['address']
        rec['address'] = address

        if address:
            rec['state'] = address.split(',')[-1].strip()

        if not exists(Station, code=rec['code']):
            db.add(Station(**rec))

    db.commit()


def process_trains():
    debug("Processing Trains")
    db.query(Train).delete() # Truncate Train
    db.commit()

    count = db.query(func.count(distinct(TempTrain.number))).scalar()
    for i, row in enumerate(db.query(TempTrain.number).distinct()):
        number = row.number
        debug("Processing train %s (%s of %s)" % (number, i, count))
        train = db.query(TempTrain).filter_by(number=number).first()
        train = clean(train)

        rec = {}
        rec['number'] = number
        rec['name'] = train['name']
        rec['type'] = train['type']
        rec['zone'] = train['zone']
        rec['from_station_code'] = train['from_station']
        rec['to_station_code'] = train['to_station']
        rec['departure'] = train['dep']
        rec['arrival'] = train['arr']
        rec['duration'] = train['duration']
        rec['number_of_halts'] = train['halts']
        rec['distance'] = train['distance']
        rec['date_from'] = train['date_from']
        rec['date_to'] = train['date_to']
        rec['return_train'] = train['return_train']

        f = db.query(Station).filter_by(code=rec['from_station_code']).first()
        if f:
            rec['from_station_name'] = f.name
        t = db.query(Station).filter_by(code=rec['to_station_code']).first()
        if t:
            rec['to_station_name'] = t.name

        classes = train['classes']
        classes = classes.replace('Classes:', '')
        classes = classes.split()

        rec['classes'] = ' '.join(classes)
        rec['first_ac'] = '1A' in classes
        rec['second_ac'] = '2A' in classes
        rec['third_ac'] = '3A' in classes
        rec['first_class'] = 'FC' in classes
        rec['sleeper'] = 'SL' in classes
        rec['chair_car'] = 'CC' in classes
        rec['second_sitting'] = '2S' in classes

        days = train.get('days') or '0000000'
        rec['departure_days'] = days.replace('0', '-')
        rec['sunday'] = days[0] != '0'
        rec['monday'] = days[1] != '0'
        rec['tuesday'] = days[2] != '0'
        rec['wednesday'] = days[3] != '0'
        rec['thursday'] = days[4] != '0'
        rec['friday'] = days[5] != '0'
        rec['saturday'] = days[6] != '0'

        db.add(Train(**rec))

    db.commit()


def main():
    process_schedule()
    process_stations()
    process_trains()


if __name__ == '__main__':
    main()
