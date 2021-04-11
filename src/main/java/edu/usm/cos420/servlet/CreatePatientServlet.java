package edu.usm.cos420.servlet;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.usm.cos420.dao.PatientCloudSqlDao;
import edu.usm.cos420.dao.PatientDao;
import edu.usm.cos420.domain.Patient;

@WebServlet(urlPatterns = {"/create"})

public class CreatePatientServlet extends HttpServlet{
	@Override
	public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	IOException {
		req.setAttribute("action", "Add");          // Part of the Header in form.jsp
		req.setAttribute("destination", "create");  // The urlPattern to invoke (this Servlet)
		req.setAttribute("page", "form");           // Tells base.jsp to include form.jsp
		req.getRequestDispatcher("/base.jsp").forward(req, resp);
	}

	@Override
	public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
	IOException {
		Patient patient = new Patient();
		patient.setFirstName(req.getParameter("firstName"));
		patient.setLastName(req.getParameter("lastName"));
		patient.setGender(req.getParameter("gender"));
		patient.setAddress(req.getParameter("address"));
		patient.setBirthDate(Date.valueOf(req.getParameter("birthDate")));

		//Get DB information
		Properties properties = new Properties();
		properties.load(getClass().getClassLoader().getResourceAsStream("database.properties"));

		String dbUrl = String.format(this.getServletContext().getInitParameter("sql.urlRemote"),
				properties.getProperty("sql.dbName"), properties.getProperty("sql.instanceName"),
				properties.getProperty("sql.userName"), properties.getProperty("sql.password"));

		PatientDao dao = null;
		try {
			dao = new PatientCloudSqlDao(dbUrl);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		try {
			Long id = dao.createPatient(patient);
			resp.sendRedirect("/read?id=" + id.toString());   // read what we just wrote
		} catch (Exception e) {
			throw new ServletException("Error creating patient", e);
		}
	}
}
