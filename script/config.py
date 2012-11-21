import os
import tempfile
import sqlite3
import MySQLdb
import datetime

version = '1.3.0b'

sched = ['0930', '1310', '1400', '1630', '1730']  # scheduling parameters for sync task

# List of clinic ids to send data for; if present, ONLY data for these clinics
# will accumulate in the staging db and, subsequently, be sent to the MOH
# server.  If empty or None, data for all clinics will be sent.
clinics = []

# path to the Lab database
import os.path
base_path = os.path.dirname(os.path.abspath(__file__))

staging_db_path = os.path.join(base_path, 'rapidsms_results.db3')

prod_db_path = None  # temporary path defined in __init__
prod_db_provider = sqlite3
prod_db_opts = {'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}

log_path = os.path.join(base_path, 'extract.log')

# the name of the table in the production database containing the results
prod_db_table = 'pcr_logbook'
# the name of the column in prod_db_table containing the lab-based ID of the record
prod_db_id_column = 'serial_no'
prod_db_date_column = 'pcr_report_date'

# the name of the table in the production database containing the results
lims_db_table = 'samples'
# the name of the column in prod_db_table containing the lab-based ID of the record
lims_db_id_column = 'ID'
lims_db_date_column = 'datetested'

# a list of the column names to select from the lab database, in the following
# order: sample_id, patient_id, facility_code, collected_on, received_on,
# processed_on, result, rejected (boolean), rejection_reason,
# reject_reason_other, birthdate, child_age, child_age_unit, sex, mother_age,
# health_worker, health_worker_title, verified, care_clinic_no
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
  'care_clinic_no'
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
  'invalid': '',  # usually means there was no sample, so there's no result
  ' ': '',  # empty sample
  'idn': '?',  # always retested, should never be verified as-is
  'ind': '?',  # always retested, should never be verified as-is
  'indeterminate': '?',  # always retested, should never be verified as-is
  'negative': '-',
#  'Sample rejected': 'rejected',
}

#production rapidsms server at MoH
submit_url = 'https://malawi-qa.projectmwana.org/labresults/incoming/'

# set these in local_config.py
auth_params = dict(realm='Lab Results', user='', passwd='')

always_on_connection = True       # if True, assume computer 'just has' internet

result_window = 365    # number of days to listen for further changes after a definitive result has been reported
unresolved_window = 365  # number of days to listen for further changes after a non-definitive result has been
                         # reported (indeterminate, inconsistent)
testing_window = 365   # number of days after a requisition forms has been entered into the system to wait for a
                       # result to be reported
init_lookback = None   # when initializing the system, from when to send the data, YYYY-mm-dd,
                       # results for (everything before that is 'archived').  if None, no archiving is done.
transport_chunk = 5000  # maximum size per POST to rapidsms server (bytes) (approximate)
send_compressed = False  # if True, payloads will be sent bz2-compressed
compression_factor = .2  # estimated compression factor


#wait times if exception during db access (minutes)
db_access_retries = [2, 3, 5, 5, 10]

#wait times if error during http send (seconds)
send_retries = [0, 0, 0, 30, 30, 30, 60, 120, 300, 300]

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


def get_unique_id(log, sample_id):
    global source_id
    sample_id = str(sample_id) + source_id
    return sample_id


def _fac_id(log, patient_id):
    if patient_id and '-' in patient_id:
        fac_id = patient_id.split('-')[0]
    else:
        log.warning('failed to parse fac_id from patient_id %s' % patient_id)
        fac_id = None
    return fac_id


def bootstrap(log):
    """creates a temporary sqlite-based production db to speed prod db queries"""
    log.debug('copy records from mysql into temp prod db')
    global prod_db_path
    _, prod_db_path = tempfile.mkstemp()
    # connect to MySQL
    mysql_db = MySQLdb.connect('localhost', 'mwana', 'mwana-labs', 'eid_malawi')
    mysql_curs = mysql_db.cursor()
    prod_db = sqlite3.connect(prod_db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    prod_curs = prod_db.cursor()
    srccols = ('serial_no', 'fac_id', 'patient_id', 'qech_lab_id',
                'pcr_plate_no', 'pcr_report_date', 'result',
                'comments', 'status', 'approved', 'action', 'care_clinic_no', 'verified')
    mysql_curs.execute('select * from pcr_logbook;')
    destcols = ('serial_no', 'fac_id', 'patient_id', 'qech_lab_id',
                'pcr_plate_no', 'pcr_report_date', 'result',
                'comments', 'status', 'approved', 'action', 'care_clinic_no', 'verified')
    desttypes = [_sql_type(log, col) for col in mysql_curs.description]

    sample_id_index = destcols.index('serial_no')
    date_column_indexes = [destcols.index(col) for col in ['pcr_report_date']]

    integer_column_indexes = [destcols.index(col) for col in ['status', 'approved', 'action', 'verified']]
    # set date columns
    for idx in date_column_indexes:
        desttypes[idx] = 'date'

    # set integer columns
    for idx in integer_column_indexes:
        desttypes[idx] = 'integer'

    columns = [' '.join([nm, tp]) for nm, tp in zip(destcols, desttypes)]
    create_sql = 'CREATE TABLE "%s" (%s);' % (prod_db_table, ','.join(columns))
    log.debug('creating temp table with SQL: %s' % create_sql)
    prod_curs.execute(create_sql)
    # '?' placeholders for INSERT statement
    values = ', '.join('?' * len(columns))
    row = mysql_curs.fetchone()
    while row:
        row = [isinstance(v, basestring) and v.strip() or v for v in row]
        for idx in integer_column_indexes:
            row[idx] = int(row[idx])

        row[sample_id_index] = get_unique_id(log, row[sample_id_index])
        insert_sql = 'INSERT INTO "%s" VALUES(%s);' % (prod_db_table, values)
        prod_curs.execute(insert_sql, row)
        row = mysql_curs.fetchone()
    index_sql = 'CREATE INDEX prod_id_idx ON %s (%s);' % (prod_db_table,
                                                          prod_db_id_column)
    prod_curs.execute(index_sql)
    prod_curs.execute('select count(*) from "%s"' % prod_db_table)
    count = prod_curs.fetchone()[0]
    log.debug('finished creating temp prod db; %s records inserted' % count)
    prod_db.commit()
    prod_curs.close()
    mysql_curs.close()


def teardown(log):
    log.debug('removing temp prod db at %s' % prod_db_path)
    if prod_db_path and os.path.exists(prod_db_path):
        os.remove(prod_db_path)
