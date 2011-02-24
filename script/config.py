import os
import tempfile
import sqlite3
import pyodbc
import datetime

version = '1.2.0b'

sched = ['0930', '1310', '1400', '1630', '1730']  #scheduling parameters for sync task

# List of clinic ids to send data for; if present, ONLY data for these clinics 
# will accumulate in the staging db and, subsequently, be sent to the MOH 
# server.  If empty or None, data for all clinics will be sent.
clinics = []

#path to the Lab database                                        
import os.path
base_path = os.path.dirname(os.path.abspath(__file__))

staging_db_path = os.path.join(base_path, 'rapidsms_results.db3')

prod_db_path = None # temporary path defined in __init__
prod_db_provider = sqlite3
prod_db_opts = {'detect_types': sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES}

prod_excel_path = r'C:\EID Masterfile\DNA-Masterbase file.xls'
prod_excel_dsn = 'Driver={Microsoft Excel Driver (*.xls)};FIRSTROWHASNAMES=1;READONLY=1;DBQ=%s' % prod_excel_path
prod_excel_opts = {'autocommit': True}

log_path = os.path.join(base_path, 'extract.log')

# the name of the table in the production database containing the results
prod_db_table = 'pcr_logbook'
# the name of the column in prod_db_table containing the lab-based ID of the record
prod_db_id_column = 'serial_no'
prod_db_date_column = 'pcr_report_date'

# a list of the column names to select from the lab database, in the following
# order: sample_id, patient_id, facility_code, collected_on, received_on,
# processed_on, result, rejected (boolean), rejection_reason,
# reject_reason_other, birthdate, child_age, child_age_unit, sex, mother_age,
# health_worker, health_worker_title, verified
prod_db_columns = [
  'patient_id',
  'fac_id',
  'NULL',
  'NULL',
  'pcr_report_date',
  'result',
  'NULL',
  'comments',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'NULL',
  'verified',
]

#date_parse = lambda x: x.date()
# sqlite gives the date to us as a datetime.date()
date_parse = lambda x: x

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
  'invalid': '', # usually means there was no sample, so there's no result
  ' ': '', # empty sample
  'idn': '?', # always retested, should never be verified as-is
  'ind': '?', # always retested, should never be verified as-is
#  'Sample rejected': 'rejected',
}

#production rapidsms server at MoH
submit_url = 'https://malawi-qa.projectmwana.org/labresults/incoming/'

# set these in local_config.py
auth_params = dict(realm='Lab Results', user='', passwd='')

always_on_connection = True       #if True, assume computer 'just has' internet

result_window = 365    #number of days to listen for further changes after a definitive result has been reported
unresolved_window = 365#number of days to listen for further changes after a non-definitive result has been
                       #reported (indeterminate, inconsistent)
testing_window = 365   #number of days after a requisition forms has been entered into the system to wait for a
                       #result to be reported
init_lookback = None   #when initializing the system, how many days back from the date of initialization to report
                       #results for (everything before that is 'archived').  if None, no archiving is done.
                      
                      
transport_chunk = 5000  #maximum size per POST to rapidsms server (bytes) (approximate)
send_compressed = False  #if True, payloads will be sent bz2-compressed
compression_factor = .2 #estimated compression factor


#wait times if exception during db access (minutes)
db_access_retries = [2, 3, 5, 5, 10]

#wait times if error during http send (seconds)
send_retries = [0, 0, 0, 30, 30, 30, 60, 120, 300, 300]

#source_tag Just a tag for identification
source_tag = 'blantyre/queens'


daemon_lock = os.path.join(base_path, 'daemon.lock')
task_lock = os.path.join(base_path, 'task.lock')

script_dir = os.path.abspath(os.path.dirname(__file__))
localconfig = os.path.join(script_dir, 'local_config.py')
if os.path.exists(localconfig):
    execfile(localconfig)

def _sql_type(log, col_desc):
    """returns the database column declaration for the given column description, e.g., for use in a create statement"""
    name, type_code, display_size, internal_size, precision, scale, null_ok = col_desc
    if type_code == str:
        sql_type = 'varchar(%s)' % internal_size
    elif type_code == float:
        sql_type = 'float'
    else:
        log.warning('SQL type for %s unknown; using varchar(255)' % type_code)
        sql_type = 'varchar(255)'
    return sql_type

def _date_parse(log, date_str):
    """attempts to parse a date from the Excel file, possibly in some random format"""
    date_str = date_str.replace('/', '-')
    date_str = date_str.replace(' ', '-')
    try:
        result = datetime.datetime.strptime(date_str, '%d-%m-%Y')
    except ValueError:
        log.warning('failed to parse date %s' % date_str)
        result = None
    return result and datetime.date(result.year, result.month, result.day)

def _fac_id(log, patient_id):
    if patient_id and '-' in patient_id:
        fac_id = patient_id.split('-')[0]
    else:
        log.warning('failed to parse fac_id from patient_id %s' % patient_id)
        fac_id = None
    return fac_id

def _debug_excel_file(log):
    log.debug('debugging prod_excel_path:')
    log.debug('path: %s' % prod_excel_path)
    file_exists = os.path.exists(prod_excel_path)
    log.debug('exists: %s' % file_exists)
    if file_exists:
        log.debug('size: %s' % os.path.getsize(prod_excel_path))
        log.debug('mtime: %s' % os.path.getmtime(prod_excel_path))
    dir_path = os.path.dirname(prod_excel_path)
    log.debug('dir_path: %s' % dir_path)
    dir_exists = os.path.exists(dir_path)
    log.debug('dir_exists: %s' % dir_exists)
    if dir_exists:
        log.debug('listdir: %s' % os.listdir(dir_path))

def bootstrap(log):
    """creates a temporary sqlite-based production db to speed prod db queries"""
    _debug_excel_file(log)
    log.debug('moving excel spreadsheet into temp prod db')
    global prod_db_path
    _, prod_db_path = tempfile.mkstemp()
    excel_db = pyodbc.connect(prod_excel_dsn, **prod_excel_opts)
    excel_curs = excel_db.cursor()
    prod_db = sqlite3.connect(prod_db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    prod_curs = prod_db.cursor()
    srccols = ('[Serial No]', '[Patient  ID No   (PIN)]', '[QECH LAB ID]',
               '[PCR Plate No]', '[PCR report date]', '[Result]',
               '[PIN appearance]', '[QECH ID appearance]', '[Comments]', '[Verified to send]')
    excel_curs.execute('select %s from [PCR LogBook$]' % ','.join(srccols))
    destcols = ('serial_no', 'fac_id', 'patient_id', 'qech_lab_id',
                'pcr_plate_no', 'pcr_report_date', 'result', 'pin_appearance',
                'qech_id_appearance', 'comments', 'verified')
    desttypes = [_sql_type(log, col) for col in excel_curs.description]
    fac_id_index = destcols.index('fac_id')
    src_id_index = srccols.index('[Patient  ID No   (PIN)]')
    src_verified_index = srccols.index('[Verified to send]')
    date_column_indexes = [destcols.index(col)
                           for col in ['pcr_plate_no', 'pcr_report_date']]
    # the verified column contains strings in Excel; convert it to an integer
    # column (boolean) here
    desttypes[src_verified_index] = 'integer'
    # fac_id is not in srccols (we calculate it below), so add the type for
    # the CREATE TABLE statement here
    desttypes.insert(fac_id_index, 'varchar(10)')
    # the columns in Excel are not date columns, so correct the types here.
    # this has no effect in sqlite3, but is kept in case another db is used
    for idx in date_column_indexes:
        desttypes[idx] = 'date'
    columns = [' '.join([nm, tp]) for nm, tp in zip(destcols, desttypes)]
    create_sql = 'CREATE TABLE "%s" (%s);' % (prod_db_table, ','.join(columns))
    log.debug('creating temp table with SQL: %s' % create_sql)
    prod_curs.execute(create_sql)
    # '?' placeholders for INSERT statement
    values = ', '.join('?'*len(columns))
    row = excel_curs.fetchone()
    while row:
        row = [isinstance(v, basestring) and v.strip() or v for v in row]
        patient_id = row[src_id_index]
        verified = row[src_verified_index]
        row[src_verified_index] =\
          int(verified and verified.lower().strip() in ('y', 'yes', 'yse') or 0)
        fac_id = patient_id and _fac_id(log, patient_id) or None
        if any(row) and not fac_id:
            #log.debug('skipping row (%s) with bad fac_id' % row)
            row = excel_curs.fetchone()
            continue
        row.insert(fac_id_index, fac_id) # add fac_id
        # make date columns to something sqlite can understand
        for idx in date_column_indexes:
            if row[idx]:
                row[idx] = _date_parse(log, row[idx])
        insert_sql = 'INSERT INTO "%s" VALUES(%s);' % (prod_db_table, values)
        prod_curs.execute(insert_sql, row)
        row = excel_curs.fetchone()
    index_sql = 'CREATE INDEX prod_id_idx ON %s (%s);' % (prod_db_table,
                                                          prod_db_id_column)
    prod_curs.execute(index_sql)
    prod_curs.execute('select count(*) from "%s"' % prod_db_table)
    count = prod_curs.fetchone()[0]
    log.debug('finished creating temp prod db; %s records inserted' % count)
    prod_db.commit()
    prod_curs.close()
    excel_curs.close()

def teardown(log):
    log.debug('removing temp prod db at %s' % prod_db_path)
    if prod_db_path and os.path.exists(prod_db_path):
        os.remove(prod_db_path)

