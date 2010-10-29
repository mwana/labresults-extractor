version = '1.2.0b'

sched = ['0930', '1310', '1600']  #scheduling parameters for sync task

# List of clinic ids to send data for; if present, ONLY data for these clinics 
# will accumulate in the staging db and, subsequently, be sent to the MOH 
# server.  If empty or None, data for all clinics will be sent.
clinics = []

#path to the Lab database                                        
import os.path
base_path = os.path.dirname(os.path.abspath(__file__))

staging_db_path = os.path.join(base_path, 'rapidsms_results.db3')
prod_db_path = r'c:\Mwana\labdata.xls'
# prod_dsn = 'DRIVER={Microsoft Access Driver (*.mdb)};DBQ=%s;PWD=pcr2010' % prod_db_path
#prod_dsn = 'DRIVER={Microsoft Access Driver (*.mdb)};DBQ=%s;PWD=pcr2010' % prod_db_path
prod_db_dsn = 'Driver={Microsoft Excel Driver (*.xls)};FIRSTROWHASNAMES=1;READONLY=1;DBQ=%s' % prod_db_path
prod_db_opts = {'autocommit': True}
log_path = os.path.join(base_path, 'extract2.log')

# the name of the table in the production database containing the results
prod_db_table = '[PCR LogBook$]'
# the name of the column in prod_db_table containing the lab-based ID of the record
prod_db_id_column = '[Serial No]'
prod_db_date_column = '[PCR report date]'

# a list of the column names to select from the lab database, in the following
# order: sample_id, patient_id, facility_code, collected_on, received_on,
# processed_on, result, rejected (boolean), rejection_reason,
# reject_reason_other, birthdate, child_age, child_age_unit, sex, mother_age,
# health_worker, health_worker_title, verified
prod_db_columns = [
  '[Patient  ID No   (PIN)]',
  'NULL',
  'NULL',
  'NULL',
  '[PCR report date]',
  '[Result]',
  'NULL',
  '[Comments]',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
]

#date_parse = lambda x: x.date()
import datetime
def date_parse(x):
    try:
        result = datetime.datetime.strptime(x, '%d-%m-%Y')
    except ValueError:
        try:
            result = datetime.datetime.strptime(x, '%d/%m/%Y')
        except ValueError:
            result = None
    return result and datetime.date(result.year, result.month, result.day)

calc_facility_code = lambda patient_id: patient_id and patient_id[:4] or patient_id

# adh:
#result_map = {1: '+', 2: '-', 3: '?'}
# uth:
#result_map = {
#  'Detected': '+',
#  'Not detected': '-',
#  'Invalid': '?',
#  'Sample rejected': 'rejected',
#}
# keys should be lowercase
result_map = {
  'positive': '+',
  'neg': '-',
  'invalid': '?',
  'idn': 'rejected',
  'ind': 'rejected',
#  'Sample rejected': 'rejected',
}

#production rapidsms server at MoH
submit_url = ''

auth_params = dict(realm='Lab Results', user='', passwd='')

always_on_connection = True       #if True, assume computer 'just has' internet

result_window = 14     #number of days to listen for further changes after a definitive result has been reported
unresolved_window = 28 #number of days to listen for further changes after a non-definitive result has been
                       #reported (indeterminate, inconsistent)
testing_window = 90    #number of days after a requisition forms has been entered into the system to wait for a
                       #result to be reported

# *WARNING* unlikely to be supported on Excel because it doesn't know what date
# format (if any) the lab is using and returns the wrong results
init_lookback = None     #when initializing the system, how many days back from the date of initialization to report
                       #results for (everything before that is 'archived').  if None, no archiving is done.
                      
                      
transport_chunk = 5000  #maximum size per POST to rapidsms server (bytes) (approximate)
send_compressed = False  #if True, payloads will be sent bz2-compressed
compression_factor = .2 #estimated compression factor


#wait times if exception during db access (minutes)
#db_access_retries = [2, 3, 5, 5, 10]
db_access_retries = []

#wait times if error during http send (seconds)
#send_retries = [0, 0, 0, 30, 30, 30, 60, 120, 300, 300]
send_retries = []

#source_tag Just a tag for identification
source_tag = 'lilongwe/unicef-testing'


daemon_lock = os.path.join(base_path, 'daemon.lock')
task_lock = os.path.join(base_path, 'task.lock')
