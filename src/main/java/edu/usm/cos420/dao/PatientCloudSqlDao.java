package edu.usm.cos420.dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.usm.cos420.domain.Patient;
import edu.usm.cos420.domain.Result;

public class PatientCloudSqlDao implements PatientDao {
	HikariDataSource ds=null;

	public PatientCloudSqlDao() throws SQLException {
		setupHikariDS();
		this.createPatientTable();
	}

	public void setupHikariDS() {
		String cloudDBUrl = "jdbc:postgresql:///%s";

		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Get DB information
		Properties properties = new Properties();
		try {
			properties.load(getClass().getClassLoader().getResourceAsStream("database.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		String dbUrl = String.format(cloudDBUrl,
				properties.getProperty("sql.dbName"));

	    HikariConfig config = new HikariConfig();

	    config.setJdbcUrl(dbUrl);
	    config.setUsername(properties.getProperty("sql.userName"));
		config.setPassword(properties.getProperty("sql.password"));
		config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
    	config.addDataSourceProperty("cloudSqlInstance", properties.getProperty("sql.instanceName"));


	    config.setMaximumPoolSize(5);
	    // minimumIdle is the minimum number of idle connections Hikari maintains in the pool.
	    // Additional connections will be established to meet this value unless the pool is full.
	    config.setMinimumIdle(5);
	    // [END cloud_sql_postgres_servlet_limit]

	    // [START cloud_sql_postgres_servlet_timeout]
	    // setConnectionTimeout is the maximum number of milliseconds to wait for a connection checkout.
	    // Any attempt to retrieve a connection from this pool that exceeds the set limit will throw an
	    // SQLException.
	    config.setConnectionTimeout(10000); // 10 seconds
	    // idleTimeout is the maximum amount of time a connection can sit in the pool. Connections that
	    // sit idle for this many milliseconds are retried if minimumIdle is exceeded.
	    config.setIdleTimeout(600000); // 10 minutes
	    // [END cloud_sql_postgres_servlet_timeout]

	    config.addDataSourceProperty("ipTypes", "PUBLIC,PRIVATE");
	    
	    // [START cloud_sql_postgres_servlet_backoff]
	    // Hikari automatically delays between failed connection attempts, eventually reaching a
	    // maximum delay of `connectionTimeout / 2` between attempts.
	    // [END cloud_sql_postgres_servlet_backoff]

	    // [START cloud_sql_postgres_servlet_lifetime]
	    // maxLifetime is the maximum possible lifetime of a connection in the pool. Connections that
	    // live longer than this many milliseconds will be closed and reestablished between uses. This
	    // value should be several minutes shorter than the database's timeout value to avoid unexpected
	    // terminations.
	    config.setMaxLifetime(1800000); // 30 minutes
	    // [END cloud_sql_postgres_servlet_lifetime]
	    
	    ds = new HikariDataSource(config);
		

	}
	public void createPatientTable() throws SQLException {
		
		try(Connection conn = ds.getConnection()){
			String createDbQuery =  "CREATE TABLE IF NOT EXISTS patients ( id SERIAL PRIMARY KEY, "
					+ "firstName VARCHAR(255), lastName VARCHAR(255), gender VARCHAR(255), address VARCHAR(255), birthDate DATE)";
			
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(createDbQuery);

			if(conn != null)
				conn.close();
		}
	}

	@Override
	public Long createPatient(Patient patient) throws SQLException {
		Long id = 0L;
		final String createPatientString = "INSERT INTO patients "
				+ "(firstName, lastName, gender, address, birthDate) "
				+ "VALUES (?, ?, ?, ?, ?)";
		try (Connection conn = ds.getConnection();
				final PreparedStatement createPatientStmt = conn.prepareStatement(createPatientString,
						Statement.RETURN_GENERATED_KEYS)) {
			createPatientStmt.setString(1, patient.getFirstName());
			createPatientStmt.setString(2, patient.getLastName());
			createPatientStmt.setString(3, patient.getGender());
			createPatientStmt.setString(4, patient.getAddress());
			createPatientStmt.setDate(5, (Date) patient.getBirthDate());

			createPatientStmt.executeUpdate();
			try (ResultSet keys = createPatientStmt.getGeneratedKeys()) {
				keys.next();
				id = keys.getLong(1);
			}
		}
		
		return id;
	}

	@Override
	public Patient readPatient(Long patientId) throws SQLException {
		final String readPatientString = "SELECT * FROM patients WHERE id = ?";
		try (Connection conn = ds.getConnection();
		    PreparedStatement readPatientStmt = conn.prepareStatement(readPatientString)) {
			readPatientStmt.setLong(1, patientId);
			try (ResultSet keys = readPatientStmt.executeQuery()) {
				keys.next();
				Patient patient = new Patient();
				patient.setId(keys.getInt(1));
				patient.setFirstName(keys.getString(2));
				patient.setLastName(keys.getString(3));
				patient.setGender(keys.getString(4));
				patient.setAddress(keys.getString(5));
				patient.setBirthDate(keys.getDate(6));

				return patient;
			}
		}
	}

	@Override
	public void updatePatient(Patient patient) throws SQLException {
		final String updatePatientString = "UPDATE patients SET firstName = ?, lastName = ?, gender = ?, address = ?, birthDate = ?  WHERE id = ?";
		try (Connection conn = ds.getConnection();
			PreparedStatement updatePatientStmt = conn.prepareStatement(updatePatientString)) {
			updatePatientStmt.setString(1, patient.getFirstName());
			updatePatientStmt.setString(2, patient.getLastName());
			updatePatientStmt.setString(3, patient.getGender());
			updatePatientStmt.setString(4, patient.getAddress());
			updatePatientStmt.setDate(5, (Date) patient.getBirthDate());
			updatePatientStmt.setLong(6, patient.getId());
			updatePatientStmt.executeUpdate();
		}

	}

	@Override
	public void deletePatient(Long patientId) throws SQLException {
		final String deletePatientString = "DELETE FROM patients WHERE id = ?";
		try (Connection conn = ds.getConnection();
			PreparedStatement deletePatientStmt = conn.prepareStatement(deletePatientString)) {
			deletePatientStmt.setLong(1, patientId);
			deletePatientStmt.executeUpdate();
		}
	}

	@Override
	public Result<Patient> listPatients(String cursor) throws SQLException {
		int offset = 0;
		int totalNumRows = 0;
		if (cursor != null && !cursor.equals("")) {
			offset = Integer.parseInt(cursor);
		}
		
		final String listPatientsString = "SELECT id, firstName, lastName, gender, address, birthDate, count(*) OVER() AS total_count FROM patients ORDER BY lastName, firstName ASC "
				+ "LIMIT 10 OFFSET ?";
		try (Connection conn = ds.getConnection();
				PreparedStatement listPatientStmt = conn.prepareStatement(listPatientsString)) {
			listPatientStmt.setInt(1, offset);
			List<Patient> resultPatients = new ArrayList<>();

			try (ResultSet rs = listPatientStmt.executeQuery()) {
				while (rs.next()) {
					Patient patient = new Patient();
					patient.setId(rs.getInt(1));
					patient.setFirstName(rs.getString(2));
					patient.setLastName(rs.getString(3));
					patient.setGender(rs.getString(4));
					patient.setAddress(rs.getString(5));
					patient.setBirthDate(rs.getDate(6));

					resultPatients.add(patient);

					totalNumRows = rs.getInt("total_count");
				}
			}

			if (totalNumRows > offset + 10) {
				return new Result<>(resultPatients, Integer.toString(offset + 10));
			} else {
				return new Result<>(resultPatients);
			}
		}
	}

}

