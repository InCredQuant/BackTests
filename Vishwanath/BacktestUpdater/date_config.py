from datetime import datetime

last_updated_date = '2025-06-18'
last_date_obj = datetime.strptime(last_updated_date, '%Y-%m-%d')
lastupdated_asof = last_date_obj.strftime('%d%m%Y')

enddate = '2025-07-31'
date_obj = datetime.strptime(enddate, '%Y-%m-%d')
updated_asof = date_obj.strftime('%d%m%Y')

futuremodels_enddate = '2025-07-31'
futurelastdate = datetime.strptime(futuremodels_enddate, '%Y-%m-%d')
