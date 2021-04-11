package edu.usm.cos420.dao;

import java.sql.SQLException;
import java.util.ArrayList;

import edu.usm.cos420.domain.Patient;
import edu.usm.cos420.domain.Result;

public interface PatientDao {
	Long createPatient(Patient patient) throws SQLException;

	Patient readPatient(Long patientId) throws SQLException;

	void updatePatient(Patient patient) throws SQLException;

	void deletePatient(Long patientId) throws SQLException;

	Result<Patient> listPatients(String startCursor) throws SQLException;
}
