import simplejson
from models import *

tables = [Station, Train, Schedule]

# Export to json
# This ugly code to save RAM when dumping half million records
for table in tables:
    table_name = table.__tablename__
    f = open("data/%s.json" % table_name, "w")
    f.write("[")
    count = db.query(func.count(table.id)).scalar()
    for i, rec in enumerate(db.query(table.id)):
        debug("Dumping %s # %s of %s" % (table_name, i, count))
        row = db.query(table).get(rec.id)
        d = row2dict(row)
        simplejson.dump(d, f)
        f.write(",")
    f.seek(-1, 2) # Overwrite the last ,
    f.write("]")
    f.close()

