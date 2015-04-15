CREATE TABLE samples (
      sample_id varchar(10) primary key,
      imported_on date,
      resolved_on date,
      patient_id varchar(100),
      facility_code varchar(10),
      collected_on date,
      received_on date,
      processed_on date,
      result varchar(20),
      result_detail varchar(100),
      birthdate date,
      child_age int,
      child_age_unit varchar(20),
      health_worker varchar(50),
      health_worker_title varchar(50),
      verified int,
      care_clinic_no varchar(50),
      phone varchar(15),
      sex varchar(14),
      sync_status varchar(10) not null default 'new'
);
CREATE USER 'rapidsms'@'localhost' IDENTIFIED BY 'rapidsms-results';

GRANT USAGE ON * . * TO 'rapidsms'@'localhost' IDENTIFIED BY 'rapidsms-results' WITH MAX_QUERIES_PER_HOUR 0 MAX_CONNECTIONS_PER_HOUR 0 MAX_UPDATES_PER_HOUR 0 MAX_USER_CONNECTIONS 0 ;

GRANT ALL PRIVILEGES ON `rapidsms_results` . * TO 'rapidsms'@'localhost';
