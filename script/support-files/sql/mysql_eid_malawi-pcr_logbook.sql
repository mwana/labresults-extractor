CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `pcr_logbook` AS select `samples`.`ID` AS `serial_no`,
`facilitys`.`facilitycode` AS `fac_id`,
`samples`.`EIDregNo` AS `patient_id`,
`samples`.`LabID` AS `qech_lab_id`,
`samples`.`plate` AS `pcr_plate_no`,
`samples`.`datetested` AS `pcr_report_date`,
`results`.`Name` AS `result`,
`samples`.`labcomment` AS `comments`,
`samples`.`status` AS `status`,
`samples`.`approved` AS `approved`,
 `samples`.`action` AS `action`,
 `samples`.`careclinicno` AS `care_clinic_no`,
 `samples`.`datecollected` AS `collected_on`,
 `samples`.`datereceived` AS `received_on`,
 `patients`.`DOB` AS `birthdate`,
 `patients`.`gender` AS `sex`,
`mothers`.`phoneno` AS `phone`,
if(((`samples`.`action` = 2) and `samples`.`approved`),1,0) AS `verified`
from (((((`results` join `samples`) join `samplestatus`) join `facilitys`) join `patients`) join `mothers`)
where ((`results`.`ID` = `samples`.`result`)
and (`samplestatus`.`ID` = `samples`.`status`)
and (`facilitys`.`ID` = `samples`.`facility`)
and (`patients`.`AutoID` = `samples`.`patientautoID`)
and (`patients`.`mother` = `mothers`.`ID`))
; --
